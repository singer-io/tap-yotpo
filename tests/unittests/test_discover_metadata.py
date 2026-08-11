import unittest

from tap_yotpo.discover import discover


class TestDiscoverMetadata(unittest.TestCase):
    """Unit tests for metadata emitted by discovery catalog."""

    @staticmethod
    def _root_metadata(stream_entry):
        for metadata_entry in stream_entry.get("metadata", []):
            if metadata_entry.get("breadcrumb") in ((), []):
                return metadata_entry.get("metadata", {})
        return {}

    def test_discover_includes_parent_metadata_for_child_streams(self):
        catalog = discover().to_dict()
        stream_map = {stream["stream"]: stream for stream in catalog.get("streams", [])}

        self.assertEqual(
            self._root_metadata(stream_map["order_fulfillments"]).get("parent-tap-stream-id"),
            "orders",
        )
        self.assertEqual(
            self._root_metadata(stream_map["product_reviews"]).get("parent-tap-stream-id"),
            "products",
        )
        self.assertEqual(
            self._root_metadata(stream_map["product_variants"]).get("parent-tap-stream-id"),
            "products",
        )

    def test_discover_sets_replication_metadata_on_stream_roots(self):
        catalog = discover().to_dict()

        for stream_entry in catalog.get("streams", []):
            root_metadata = self._root_metadata(stream_entry)
            self.assertIn("forced-replication-method", root_metadata)
            self.assertIn("replication-method", root_metadata)

    def test_discover_includes_key_properties_for_all_streams(self):
        catalog = discover().to_dict()

        for stream_entry in catalog.get("streams", []):
            self.assertIn("key_properties", stream_entry)
            self.assertIsInstance(stream_entry["key_properties"], list)
            self.assertGreater(len(stream_entry["key_properties"]), 0)

    def test_discover_key_properties_match_root_table_keys(self):
        catalog = discover().to_dict()

        for stream_entry in catalog.get("streams", []):
            root_metadata = self._root_metadata(stream_entry)
            self.assertEqual(
                stream_entry.get("key_properties", []),
                root_metadata.get("table-key-properties", []),
            )
