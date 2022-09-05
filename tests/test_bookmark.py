import tap_tester.connections as connections
import tap_tester.runner as runner
from base import YotpoBaseTest
from tap_tester import menagerie
from tap_tester.logger import LOGGER

from datetime import timedelta
from datetime import datetime as dt
import dateutil.parser

class YotpoBookMarkTest(YotpoBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""
    
    def name(self):
        return "tap_tester_yotpo_bookmark_test"

    def test_run(self):
        """
        Verify that for each stream you can do a sync which records bookmarks.
        That the bookmark is the maximum value sent to the target for the replication key.
        That a second sync respects the bookmark
            All data of the second sync is >= the bookmark from the first sync
            The number of records in the 2nd sync is less then the first (This assumes that
                new data added to the stream is done at a rate slow enough that you haven't
                doubled the amount of data from the start date to the first sync between
                the first sync and second sync run in this test)

        Verify that for full table stream, all data replicated in sync 1 is replicated again in sync 2.

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """
        
        expected_streams = {'emails', 'reviews', 'products', 'unsubscribers', 'customers', 'product_reviews'} #{'product_reviews'}
        #{'emails', 'reviews', 'products', 'unsubscribers', 'customers'} #self.expected_streams()  'unsubscribers', 'customers', 
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()
        LOGGER.info("self.get_properties() : %s",self.get_properties())
        expected_email_lookback_window = -1 * int(self.get_properties()['email_stats_lookback_days'])  # lookback window
        expected_review_lookback_window = -1 * int(self.get_properties()['reviews_lookback_days']) 
        ##########################################################################
        # First Sync
        ##########################################################################
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                           if catalog.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        LOGGER.info("bbbbbbb first_sync_bookmarks: %s",first_sync_bookmarks)

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################

        new_states = {'bookmarks': dict()}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)
        LOGGER.info("...........simulated_states : %s",simulated_states)
        for stream, new_state in simulated_states.items():
            new_states['bookmarks'][stream] = new_state
        menagerie.set_state(conn_id, new_states)

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Test By Stream
        ##########################################################################


        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_replication_method = expected_replication_methods[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(
                                           stream, {}).get('messages', [])
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(
                                            stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']
                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)

                LOGGER.info("************first_bookmark_key_value : %s",first_bookmark_key_value)
                

                if expected_replication_method == self.INCREMENTAL :

                    # collect information specific to incremental streams from syncs 1 & 2
                    replication_key = next(iter(expected_replication_keys[stream]))
                    
                    LOGGER.info("---------- new_states : %s , type : %s",new_states, type(new_states))
                    if stream == 'product_reviews' :
                        first_bookmark_value_utc = {key : self.convert_state_to_utc(value) for key, value in first_bookmark_key_value.items()}
                        second_bookmark_value_utc = {key : self.convert_state_to_utc(value) for key, value in second_bookmark_key_value.items()}
                        simulated_bookmark_value = { key : self.convert_state_to_utc(value) for key, value in new_states['bookmarks'][stream].items()}

                    else :
                        first_bookmark_value = first_bookmark_key_value.get(replication_key)
                        LOGGER.info("************  first_bookmark_value : %s",first_bookmark_value)
                        second_bookmark_value = second_bookmark_key_value.get(replication_key)
                        first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                        second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)
                        simulated_bookmark_value = self.convert_state_to_utc(new_states['bookmarks'][stream][replication_key])
                    LOGGER.info("---------- simulated_bookmark_value : %s , type : %s",simulated_bookmark_value, type(simulated_bookmark_value))
                    
                    if stream == 'emails' :
                        simulated_bookmark_minus_lookback = self.timedelta_formatted(simulated_bookmark_value,
                                                                                days=expected_email_lookback_window) 
                    elif stream == 'reviews' :
                        simulated_bookmark_minus_lookback = self.timedelta_formatted(simulated_bookmark_value,
                                                                                days=expected_review_lookback_window)
                    else :
                        simulated_bookmark_minus_lookback = simulated_bookmark_value    

                    LOGGER.info("********* simulated_bookmark_minus_lookback: %s, type : %s",simulated_bookmark_minus_lookback, type(simulated_bookmark_minus_lookback))
                    
                    if stream == 'product_reviews' :
                        # Verify the first sync sets a bookmark of the expected form
                        self.assertIsNotNone(first_bookmark_key_value)

                        # Verify the second sync sets a bookmark of the expected form
                        self.assertIsNotNone(second_bookmark_key_value)

                        # Verify the second sync bookmark is Equal to the first sync bookmark
                        # assumes no changes to data during test
                        for item in first_bookmark_key_value.keys():
                            #LOGGER.info("item.......:%s",item)
                            self.assertGreaterEqual(second_bookmark_key_value.get(item), first_bookmark_key_value.get(item))
                        
                        # LOGGER.info("================== all_products_first :%s",all_products_first)
                        LOGGER.info("================== first_sync_messages :%s",first_sync_messages)
                        LOGGER.info("+++++++++++++++++ first_bookmark_value_utc: %s",first_bookmark_value_utc)
                       # all_child_first = []
                        for record in first_sync_messages:
                            #LOGGER.info("================== record :%s",record)
                            #all_child_first.append({record['domain_key']: record.get(replication_key)})
                            replication_key_value = record.get(replication_key)
                            for item,value in first_bookmark_value_utc.items() :
                                 if item == record['domain_key'] :
                                    first_bookmark_value_utc_value = value
                                    self.assertLessEqual(replication_key_value,
                                                first_bookmark_value_utc_value,
                                                msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")                

                        LOGGER.info("================== second_sync_messages :%s",second_sync_messages)
                        for record in second_sync_messages:
                            # 
                            replication_key_value = record.get(replication_key)
                            for item,value in simulated_bookmark_minus_lookback.items() :
                                 if item == record['domain_key'] :
                                    simulated_bookmark_minus_lookback_value = value
                                    self.assertGreaterEqual(replication_key_value,
                                                            simulated_bookmark_minus_lookback_value,
                                                            msg="Second sync records do not repect the previous bookmark.")

                            for item,value in second_bookmark_value_utc.items() :
                                 if item == record['domain_key'] :
                                    second_bookmark_value_utc_value = value
                                    # Verify the second sync bookmark value is the max replication key value for a given stream
                                    self.assertLessEqual(replication_key_value,
                                                second_bookmark_value_utc_value,
                                                msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")

                    else :
                        # Verify the first sync sets a bookmark of the expected form
                        self.assertIsNotNone(first_bookmark_key_value)
                        self.assertIsNotNone(first_bookmark_value)

                        # Verify the second sync sets a bookmark of the expected form
                        self.assertIsNotNone(second_bookmark_key_value)
                        self.assertIsNotNone(second_bookmark_value)

                        # Verify the second sync bookmark is Equal to the first sync bookmark
                        # assumes no changes to data during test
                        self.assertEqual(second_bookmark_value, first_bookmark_value)

                        LOGGER.info("================== first_sync_messages :%s",first_sync_messages)
                        for record in first_sync_messages:
                            # Verify the first sync bookmark value is the max replication key value for a given stream
                            replication_key_value = record.get(replication_key)
                            LOGGER.info("********* replication_key_value: %s, type : %s",replication_key_value, type(replication_key_value))
                            self.assertLessEqual(replication_key_value,
                                                first_bookmark_value_utc,
                                                msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")

                        LOGGER.info("================== second_sync_messages :%s",second_sync_messages)
                        for record in second_sync_messages:
                            # 
                            replication_key_value = record.get(replication_key)
                            self.assertGreaterEqual(replication_key_value,
                                                    simulated_bookmark_minus_lookback,
                                                    msg="Second sync records do not repect the previous bookmark.")

                            # Verify the second sync bookmark value is the max replication key value for a given stream
                            self.assertLessEqual(replication_key_value,
                                                second_bookmark_value_utc,
                                                msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")

                    LOGGER.info("------------- first_sync_count : %s",first_sync_count)
                    LOGGER.info("------------- second_sync_count : %s",second_sync_count)
                    # verify that you get less than or equal to data getting at 2nd time around
                    self.assertLessEqual(second_sync_count,
                                        first_sync_count,
                                        msg="second sync didn't have less records, bookmark usage not verified")

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)
                    LOGGER.info("------------- first_sync_count : %s",first_sync_count)
                    LOGGER.info("------------- second_sync_count : %s",second_sync_count)
                else:

                    raise NotImplementedError("INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(stream,
                                                                                                                expected_replication_method))

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))