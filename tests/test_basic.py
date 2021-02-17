import os
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
from functools import reduce
import unittest

class Yotpo(unittest.TestCase):
    def name(self):
        return "tap_tester_yotpo"

    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_YOTPO_API_KEY'),
                                    os.getenv('TAP_YOTPO_API_SECRET')] if x == None]
        if len(missing_envs) != 0:
            raise Exception("set TAP_YOTPO_API_KEY, TAP_YOTPO_API_SECRET")

    def get_type(self):
        return "platform.yotpo"

    @staticmethod
    def tap_name():
        return "yotpo"

    def expected_check_streams(self):
        return {
            'products',
            'reviews'
        }

    def expected_sync_streams(self):
        return {
            'products',
            'reviews'
        }

    def expected_pks(self):
        return {
            'products': {"id"},
            'reviews': {"id"}
        }

    def get_credentials(self):
        return {'api_key': os.getenv('TAP_YOTPO_API_KEY'),
                'api_secret': os.getenv('TAP_YOTPO_API_SECRET')
        }

    def get_properties(self):
        return {
            'start_date' : '2017-01-01T00:00:00Z'
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # Run the tap in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify the check's exit status
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify that there are catalogs found
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        # Select some catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]
        for catalog in our_catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema)

        # Clear State and run sync
        menagerie.set_state(conn_id, {})
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify rows were synced
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))
