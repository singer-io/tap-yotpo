from base import YotpoBaseTest
from tap_tester import connections, menagerie, runner

# As we are not able to generate following fields by yotpo post apis, so removed it form expectation list.
KNOWN_MISSING_FIELDS = {
    "reviews": {"user_reference"},
    "orders": {"shipping_address", "cancellation", "billing_address"},
}


class YotpoAllFields(YotpoBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    def name(self):
        return "tap_tester_yotpo_all_fields_test"

    def test_run(self):
        """
        - Verify no unexpected streams were replicated
        - Verify that more than just the automatic fields are replicated for each stream.
        - verify all fields for each stream are replicated
        """

        # Streams to verify all fields tests
        expected_streams = self.expected_streams()

        expected_automatic_fields = self.expected_automatic_fields()
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_all_fields = [
            catalog for catalog in found_catalogs if catalog.get("tap_stream_id") in expected_streams
        ]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_all_fields)

        # Grab metadata after performing table-and-field selection to set expectations
        # used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog["stream_id"], catalog["stream_name"]
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [
                md_entry["breadcrumb"][1] for md_entry in catalog_entry["metadata"] if md_entry["breadcrumb"] != []
            ]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_all_keys = stream_to_all_catalog_fields[stream] - KNOWN_MISSING_FIELDS.get(stream, set())
                expected_automatic_keys = expected_automatic_fields.get(stream, set())

                # Verify that more than just the automatic fields are replicated for each stream.
                self.assertTrue(
                    expected_automatic_keys.issubset(expected_all_keys),
                    msg=f'{expected_automatic_keys - expected_all_keys} is not in "expected_all_keys"',
                )

                messages = synced_records.get(stream)
                # Collect actual values
                actual_all_keys = set()
                for message in messages["messages"]:
                    if message["action"] == "upsert":
                        actual_all_keys.update(message["data"].keys())
                # Verify all fields for each stream are replicated
                self.assertSetEqual(expected_all_keys, actual_all_keys)
