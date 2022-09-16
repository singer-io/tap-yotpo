import unittest
import os
from datetime import timedelta
from datetime import datetime as dt
import dateutil.parser
import pytz
from typing import Dict, Set
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER

class YotpoBaseTest(unittest.TestCase):

    PRIMARY_KEYS = "table-key-properties"
    START_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    BOOKMARK_COMPARISON_FORMAT = "%Y-%m-%dT00:00:00+00:00"
    REPLICATION_KEYS = "valid-replication-keys"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    STARTDATE_KEYS = "start_date_key"
    DATETIME_FMT = {
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.000000Z"
    }

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_YOTPO_API_KEY'),
                                    os.getenv('TAP_YOTPO_API_SECRET')] if x == None]
        if len(missing_envs) != 0:
            raise Exception("Missing test-required environment variables")

    def get_type(self):
        return "platform.yotpo"

    @staticmethod
    def tap_name():
        return "yotpo"

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        return {
            'emails': {
                self.PRIMARY_KEYS: {'email_address', 'email_sent_timestamp'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'email_sent_timestamp'}
            },
            'product_reviews': {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'created_at'}
            },
            'products': {
                self.PRIMARY_KEYS: {'yotpo_id'},
                self.REPLICATION_METHOD: self.FULL_TABLE,
            },
            'reviews': {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
            'unsubscribers': {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.FULL_TABLE,
            },
            'orders': {
                self.PRIMARY_KEYS: {'yotpo_id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'order_date'}
            },
            'order_fulfillments': {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
            'product_variants': {
                self.PRIMARY_KEYS: {'yotpo_id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            },
            'collections': {
                self.PRIMARY_KEYS: {'yotpo_id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'updated_at'}
            }
        }

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def parse_date(self, date_value):
        """
        Pass in string-formatted-datetime, parse the value, and return it as an unformatted datetime object.
        """
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d"
        }
        for date_format in date_formats:
            try:
                date_stripped = dt.strptime(date_value, date_format)
                return date_stripped
            except ValueError:
                continue

        raise NotImplementedError("Tests do not account for dates of this format: {}".format(date_value))

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def get_credentials(self):
        return {'api_key': os.getenv('TAP_YOTPO_API_KEY'),
                'api_secret': os.getenv('TAP_YOTPO_API_SECRET')
        }

    def get_properties(self, original: bool = True):
        return_value = {
            'start_date': '2022-07-01T00:00:00Z',
            'email_stats_lookback_days': 30,
            'reviews_lookback_days': 30
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def expected_automatic_fields(self):
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set())
        return auto_fields


    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'] , found_catalogs))

        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        LOGGER.info("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        LOGGER.info("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self,
                                                     conn_id,
                                                     test_catalogs,
                                                     select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.
        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            LOGGER.info("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    LOGGER.info("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or \
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields


    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)
            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            try:
                date_stripped = dt.strptime(dtime, self.BOOKMARK_COMPARISON_FORMAT)
                return_date = date_stripped + timedelta(days=days)
                return dt.strftime(return_date, self.BOOKMARK_COMPARISON_FORMAT)

            except ValueError:
                return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))

    def calculated_states_by_stream(self, current_state):
        timedelta_by_stream = {stream: [0,0,0,5]  # {stream_name: [days, hours, minutes, seconds], ...}
                               for stream in self.expected_streams()}
        
        stream_to_calculated_state = {stream: "" for stream in current_state['bookmarks'].keys()}
        for stream, state in current_state['bookmarks'].items():
            state_format = '%Y-%m-%dT%H:%M:%S-00:00'
            if stream in ['product_reviews','order_fulfillments','product_variants'] :
                new_state = {}
                for state_key in state.keys() :
                    state_value = next(iter(state.values()))
                    state_as_datetime = dateutil.parser.parse(state_value)
                    calculated_state_as_datetime = state_as_datetime - timedelta(*timedelta_by_stream[stream])


                    calculated_state_formatted = dt.strftime(calculated_state_as_datetime, state_format)
                    new_state[state_key] = calculated_state_formatted
                stream_to_calculated_state[stream] = new_state
            else :
                state_key, state_value = next(iter(state.keys())), next(iter(state.values()))
                state_as_datetime = dateutil.parser.parse(state_value)

                days, hours, minutes, seconds = timedelta_by_stream[stream]
                calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

                calculated_state_formatted = dt.strftime(calculated_state_as_datetime, state_format)

                stream_to_calculated_state[stream] = {state_key: calculated_state_formatted}               

        return stream_to_calculated_state

    def convert_state_to_utc(self, date_str):
        """
        Convert a saved bookmark value of the form '2020-08-25T13:17:36-07:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return dt.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def is_incremental(self, stream):
        return self.expected_metadata().get(stream).get(self.REPLICATION_METHOD) == self.INCREMENTAL

    def create_interrupt_sync_state(self, state: Dict, interrupt_stream: str, pending_streams: Set, start_date: str) -> Dict:
        """
        This function will create a new interrupt sync bookmark state
        """
        expected_replication_keys = self.expected_replication_keys()
        bookmark_state = state['bookmarks']
        if self.is_incremental(interrupt_stream):
            if interrupt_stream in {'order_fulfillments', 'product_reviews', 'product_variants'}:
                reverse_sorted_id_list = list(bookmark_state[interrupt_stream].keys())[::-1]
                breakpoint = int(len(reverse_sorted_id_list)/2)
                # update bookmark value in reverse order of keys
                for index, id in enumerate(reverse_sorted_id_list):
                    if index >= breakpoint:
                        bookmark_state[interrupt_stream]["currently_syncing"] = id
                        break
                    bookmark_date = bookmark_state[interrupt_stream][id]
                    updated_bookmark_date = self.get_mid_point_date(start_date, bookmark_date)
                    bookmark_state[interrupt_stream][id] = updated_bookmark_date
            else:
                replication_key = next(iter(expected_replication_keys[interrupt_stream]))
                bookmark_date = bookmark_state[interrupt_stream][replication_key]
                updated_bookmark_date = self.get_mid_point_date(start_date, bookmark_date)
                bookmark_state[interrupt_stream][replication_key] = updated_bookmark_date
            state["currently_syncing"] = interrupt_stream

        # for pending streams, update the bookmark_value to start-date 
        for stream in iter(pending_streams):
            # only incremental streams should have the bookmark value
            if self.is_incremental(stream):
                if stream in {'order_fulfillments', 'product_reviews', 'product_variants'}:
                    for id in bookmark_state[stream].keys():
                        bookmark_state[stream][id] = start_date
                else:
                    replication_key = next(iter(expected_replication_keys[stream]))
                    bookmark_state[stream][replication_key] = start_date
            state["bookmarks"] = bookmark_state
        
        return state

    def get_mid_point_date(self, start_date: str, bookmark_date: str) -> str:
        """
        Function to find the middle date between two dates
        """
        date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        start_date_dt = dt.strptime(start_date, date_format)
        bookmark_date_dt = dt.strptime(bookmark_date, date_format)
        mid_date_dt = start_date_dt.date() + (bookmark_date_dt-start_date_dt) / 2
        # Convert datetime object to string format
        mid_date = mid_date_dt.strftime(date_format)
        return mid_date
