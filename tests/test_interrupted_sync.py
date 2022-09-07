from datetime import datetime

from tap_tester import connections, menagerie, runner
from base import YotpoBaseTest
from tap_tester.logger import LOGGER
import copy

class YotpoInterruptedSyncTest(YotpoBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""

    def name(self):
        return "tap_tester_yotpo_bookmark_test"

    def test_run(self):
        """

        Scenario: A sync job is interrupted. The state is saved with `currently_syncing`.
                  The next sync job kicks off and the tap picks back up on that `currently_syncing` stream.
        Expected State Structure:
        {
            'currently_syncing': 'product_variants',
            'bookmarks': {
                'collections': {
                    'updated_at': '2022-09-05T14:54:55.000000Z'
                },
                'emails': {
                    'email_sent_timestamp': '2022-09-06T03:00:10.000000Z'
                },
                'order_fulfillments': {
                    '1234567890': '2022-09-02T04:32:15.000000Z',
                    '1234567890': '2022-09-02T04:32:15.000000Z'
                },
                'orders': {
                    'updated_at': '2022-09-02T14:18:36.000000Z'
                },
                'product_reviews': {
                    '1111111111': '2021-01-01T00:00:00.000000Z',
                    '2222222222': '2021-01-01T00:00:00.000000Z'
                },
                'product_variants': {
                    '66666666': '2022-08-05T09:02:07.000000Z',
                    '77777777': '2021-07-15T09:02:07.000000Z',
                    '88888888': '2021-01-01T00:00:00.000000Z',
                    'currently_syncing': '77777777'
                },
                'reviews': {
                    'updated_at': '2021-01-01T00:00:00.000000Z'
                }
            }
        }
        Test Cases:
        - Verify an interrupted sync can resume based on the currently_syncing and stream level bookmark value.
        - Verify only records with replication-key values greater than or equal to the stream level bookmark 
          are replicated on the resuming sync for the interrupted stream.
        - Verify the pending streams are replicated following the interrupted stream in the resuming sync. 
          (All pending streams must replicate before streams that were already synced)
        """
        
        start_date = self.get_properties()['start_date'].replace('Z', '.000000Z')
        expected_streams = self.expected_streams()
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()
        LOGGER.info(f"expected_replication_keys = {expected_replication_keys} \n expected_replication_methods = {expected_replication_methods}")

        ##########################################################################
        # First Sync
        ##########################################################################
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                           if catalog.get('tap_stream_id') in expected_streams]
        
        self.perform_and_verify_table_and_field_selection(
            conn_id, catalog_entries)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)
        
        LOGGER.info(f"first_sync_record_count = {first_sync_record_count}")

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################
        
        LOGGER.info("Current Bookmark after first sync: {}".format(first_sync_bookmarks))
        
        completed_streams = {'collections', 'emails', 'order_fulfillments', 'orders', 'product_reviews'}
        pending_streams = {'reviews'}
        interrupt_stream = 'product_variants'
        interrupted_sync_states = self.create_interrupt_sync_state(copy.deepcopy(first_sync_bookmarks), interrupt_stream, pending_streams, start_date)
        menagerie.set_state(conn_id, interrupted_sync_states)
        
        LOGGER.info(f"Interrupted Bookmark - {interrupted_sync_states}")

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)
        LOGGER.info(f"second_sync_record_count = {second_sync_record_count} \n second_sync_bookmarks = {second_sync_bookmarks}")
        ##########################################################################
        # Test By Stream
        ##########################################################################

        for stream in expected_streams:
            LOGGER.info(f"Executing for stream = {stream}")
            with self.subTest(stream=stream):
                # expected values
                expected_replication_method = expected_replication_methods[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(
                                            stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']
                first_bookmark_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                LOGGER.info(f"first_bookmark_value = {first_bookmark_value} \n second_bookmark_value = {second_bookmark_value}")
                
                if expected_replication_method == self.INCREMENTAL:
                    
                    replication_key = next(iter(expected_replication_keys[stream]))
                    
                    interrupted_bookmark_value = interrupted_sync_states['bookmarks'][stream]
                    if stream in completed_streams:
                        # Verify at least 1 record was replicated in the second sync
                        if stream in {'order_fulfillments', 'product_reviews', 'product_variants'}:
                            # Bookmarking logic differs compared to other streams.
                            self.assertGreaterEqual(second_sync_count,
                                            1, 
                                            msg="Incorrect bookmarking for {0}, at least one record should be replicated".format(stream))
                        elif second_bookmark_value[replication_key] == first_bookmark_value[replication_key]:
                            self.assertGreaterEqual(second_sync_count,
                                            1, 
                                            msg="Incorrect bookmarking for {0}, at least one record should be replicated".format(stream))
                        else:
                            # second sync bookmark is greater than first sync
                            self.assertGreater(second_sync_count,
                                                1,
                                                msg="Incorrect bookmarking for {0}, more than one records should be replicated if second sync bookmark is greater than first sync".format(stream))

                    elif stream == interrupted_sync_states.get('currently_syncing', None):
                        # For interrupted stream records sync count should be less equals
                        self.assertLessEqual(second_sync_count,
                                            first_sync_count,
                                            msg="For interrupted stream - {0}, seconds sync record count should be lesser or equal to first sync".format(stream))

                    elif stream in pending_streams:
                        # First sync and second sync record count match
                        self.assertGreaterEqual(second_sync_count,
                                                first_sync_count,
                                                msg="For pending sync streams - {0}, second sync record count should be more than or equal to first sync".format(stream))     

                    else:
                        raise Exception("Invalid state of stream {0} in interrupted state, please update appropriate state for the stream".format(stream))
                    
                    if stream in {'product_reviews', 'product_variants', 'order_fulfillments'}:
                        if stream == 'product_reviews' :
                            repl_key = 'product_yotpo_id'
                        elif stream == 'order_fulfillments':
                            repl_key = 'order_id' 
                        elif stream == 'product_variants':
                            repl_key = 'yotpo_product_id'
                        for record in second_sync_messages:
                            # Verify the second sync replication key value is Greater or Equal to the first sync bookmark for each id
                            replication_key_value = record.get(replication_key)
                            repl_key_id = str(record.get(repl_key))
                            interrupt_bookmark_value = interrupted_bookmark_value[repl_key_id] if repl_key_id in interrupted_bookmark_value.keys() else start_date

                            self.assertLessEqual(interrupt_bookmark_value,
                                                replication_key_value,
                                                msg="Interrupt bookmark was set incorrectly, a record with a lesser replication-key value was synced compared to interrupt bookmark value. Record = {}".format(record))
                                                # TODO: remove record from above message

                            # Verify the second sync bookmark value is the max replication key value for a given id
                            second_bookmark_id_value = second_bookmark_value[repl_key_id] if repl_key_id in second_bookmark_value.keys() else start_date
                            self.assertLessEqual(replication_key_value,
                                                second_bookmark_id_value,
                                                msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced. Record = {}".format(record))
                                                # TODO: remove record from above message
                    else:
                        for record in second_sync_messages:
                            # Verify the second sync replication key value is Greater or Equal to the first sync bookmark
                            replication_key_value = record.get(replication_key)

                            self.assertLessEqual(interrupted_bookmark_value[replication_key],
                                                replication_key_value,
                                                msg="Interrupt bookmark was set incorrectly, a record with a lesser replication-key value was synced compared to interrupt bookmark value. Record = {}".format(record))
                                                # TODO: remove record from above message

                            # Verify the second sync bookmark value is the max replication key value for a given stream
                            self.assertLessEqual(replication_key_value,
                                                second_bookmark_value[replication_key],
                                                msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced. Record = {}".format(record))
                                                # TODO: remove record from above message
                
                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_value)
                    self.assertIsNone(second_bookmark_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)
                else:

                    raise NotImplementedError("INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(stream,
                                                                                                                expected_replication_method))
