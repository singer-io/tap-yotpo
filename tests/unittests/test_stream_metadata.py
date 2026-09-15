import unittest

from tap_yotpo.streams.collections import Collections
from tap_yotpo.streams.order_fulfillments import OrderFulfillments
from tap_yotpo.streams.products import Products


class TestStreamMetadata(unittest.TestCase):
    """Unit tests for stream root metadata generation."""

    def setUp(self):
        self.simple_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "updated_at": {"type": "string", "format": "date-time"},
            },
        }

    @staticmethod
    def _root_metadata(metadata):
        for entry in metadata:
            if entry.get("breadcrumb") in ((), []):
                return entry.get("metadata", {})
        return {}

    def test_incremental_stream_root_metadata(self):
        metadata = Collections.get_metadata(self.simple_schema)
        root_md = self._root_metadata(metadata)

        self.assertEqual(root_md.get("forced-replication-method"), "INCREMENTAL")
        self.assertEqual(root_md.get("valid-replication-keys"), ["updated_at"])

    def test_child_stream_includes_parent_metadata(self):
        metadata = OrderFulfillments.get_metadata(self.simple_schema)
        root_md = self._root_metadata(metadata)

        self.assertEqual(root_md.get("parent-tap-stream-id"), "orders")
        self.assertEqual(root_md.get("forced-replication-method"), "INCREMENTAL")

    def test_full_table_stream_root_metadata(self):
        metadata = Products.get_metadata(self.simple_schema)
        root_md = self._root_metadata(metadata)

        self.assertEqual(root_md.get("forced-replication-method"), "FULL_TABLE")
        self.assertNotIn("valid-replication-keys", root_md)
