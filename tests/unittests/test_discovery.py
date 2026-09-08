"""Unit tests for tap-yotpo discovery module."""
import unittest
from unittest.mock import MagicMock, patch

from tap_yotpo.discover import _apply_access_checks, _prune_inaccessible_children, discover
from tap_yotpo.streams import STREAMS
from tap_yotpo.streams.abstracts import BaseStream

PARENT_STREAMS = [name for name, cls in STREAMS.items() if not getattr(cls, "parent", "")]
CHILD_STREAMS = [name for name, cls in STREAMS.items() if getattr(cls, "parent", "")]
SIMPLE_SCHEMA = {"type": "object", "properties": {"id": {"type": "integer"}}}


def _build_test_data():
    """Builds schemas and field_metadata dicts for all streams."""
    schemas = dict(STREAMS)
    field_metadata = {
        name: (SIMPLE_SCHEMA, cls.get_metadata(SIMPLE_SCHEMA)) for name, cls in STREAMS.items()
    }
    return schemas, field_metadata


class TestPruneInaccessibleChildren(unittest.TestCase):
    """Tests for _prune_inaccessible_children function."""

    def test_child_removed_when_parent_absent(self):
        """Child stream is removed when its parent is not in the schemas dict."""
        schemas = {"product_reviews": STREAMS["product_reviews"]}
        field_metadata = {"product_reviews": (SIMPLE_SCHEMA, [])}

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn("product_reviews", schemas)
        self.assertNotIn("product_reviews", field_metadata)

    def test_child_kept_when_parent_present(self):
        """Child stream is kept when its parent is also in the schemas dict."""
        schemas = {
            "products": STREAMS["products"],
            "product_reviews": STREAMS["product_reviews"],
        }
        field_metadata = {
            "products": (SIMPLE_SCHEMA, []),
            "product_reviews": (SIMPLE_SCHEMA, []),
        }

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertIn("product_reviews", schemas)
        self.assertIn("products", schemas)

    def test_parent_streams_never_removed(self):
        """Streams without a parent are not removed by pruning."""
        schemas = {"reviews": STREAMS["reviews"], "collections": STREAMS["collections"]}
        field_metadata = {"reviews": (SIMPLE_SCHEMA, []), "collections": (SIMPLE_SCHEMA, [])}
        original_count = len(schemas)

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertEqual(len(schemas), original_count)

    def test_both_product_children_removed_when_products_absent(self):
        """Both product_reviews and product_variants are removed when products is absent."""
        schemas = {
            "product_reviews": STREAMS["product_reviews"],
            "product_variants": STREAMS["product_variants"],
        }
        field_metadata = {
            "product_reviews": (SIMPLE_SCHEMA, []),
            "product_variants": (SIMPLE_SCHEMA, []),
        }

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn("product_reviews", schemas)
        self.assertNotIn("product_variants", schemas)

    def test_order_fulfillments_removed_when_orders_absent(self):
        """order_fulfillments is removed when orders is not in schemas."""
        schemas = {"order_fulfillments": STREAMS["order_fulfillments"]}
        field_metadata = {"order_fulfillments": (SIMPLE_SCHEMA, [])}

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn("order_fulfillments", schemas)

    def test_child_removal_warning_logged(self):
        """A warning is logged when a child stream is excluded with its parent."""
        schemas = {"product_reviews": STREAMS["product_reviews"]}
        field_metadata = {"product_reviews": (SIMPLE_SCHEMA, [])}

        with patch("tap_yotpo.discover.LOGGER") as mock_logger:
            _prune_inaccessible_children(schemas, field_metadata)

        warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
        self.assertTrue(any("product_reviews" in c and "products" in c for c in warning_calls))


class TestApplyAccessChecks(unittest.TestCase):
    """Tests for _apply_access_checks function."""

    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_client.config = {"api_key": "test_key", "api_secret": "test_secret"}

    def test_all_accessible_keeps_all_streams(self):
        """When all streams are accessible, all remain in the catalog."""
        schemas, field_metadata = _build_test_data()

        def always_accessible(self_):
            return True

        with patch.object(BaseStream, "check_access", new=always_accessible):
            _apply_access_checks(self.mock_client, schemas, field_metadata)

        self.assertEqual(set(schemas.keys()), set(STREAMS.keys()))

    def test_inaccessible_products_excludes_its_children(self):
        """When products is inaccessible, product_reviews and product_variants are also excluded."""
        schemas, field_metadata = _build_test_data()

        def check_access_fn(self_):
            if getattr(self_, "parent", ""):
                return True
            return self_.tap_stream_id != "products"

        with patch.object(BaseStream, "check_access", new=check_access_fn):
            _apply_access_checks(self.mock_client, schemas, field_metadata)

        self.assertNotIn("products", schemas)
        self.assertNotIn("product_reviews", schemas)
        self.assertNotIn("product_variants", schemas)
        self.assertIn("orders", schemas)
        self.assertIn("order_fulfillments", schemas)

    def test_inaccessible_orders_excludes_order_fulfillments(self):
        """When orders is inaccessible, order_fulfillments is also excluded."""
        schemas, field_metadata = _build_test_data()

        def check_access_fn(self_):
            if getattr(self_, "parent", ""):
                return True
            return self_.tap_stream_id != "orders"

        with patch.object(BaseStream, "check_access", new=check_access_fn):
            _apply_access_checks(self.mock_client, schemas, field_metadata)

        self.assertNotIn("orders", schemas)
        self.assertNotIn("order_fulfillments", schemas)
        self.assertIn("products", schemas)

    def test_all_parent_streams_inaccessible_raises_exception(self):
        """When all parent streams are inaccessible, an exception is raised."""
        schemas, field_metadata = _build_test_data()

        def only_children_accessible(self_):
            return bool(getattr(self_, "parent", ""))

        with patch.object(BaseStream, "check_access", new=only_children_accessible):
            with self.assertRaises(RuntimeError):
                _apply_access_checks(self.mock_client, schemas, field_metadata)

    def test_partial_access_warning_logged(self):
        """A warning is logged when a stream is excluded due to inaccessibility."""
        schemas, field_metadata = _build_test_data()

        def check_access_fn(self_):
            if getattr(self_, "parent", ""):
                return True
            return self_.tap_stream_id != "reviews"

        with patch.object(BaseStream, "check_access", new=check_access_fn), \
                patch("tap_yotpo.discover.LOGGER") as mock_logger:
            _apply_access_checks(self.mock_client, schemas, field_metadata)

        warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
        self.assertTrue(any("reviews" in c for c in warning_calls))


class TestCheckAccess(unittest.TestCase):
    """Tests for BaseStream.check_access() method."""

    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_client.config = {"api_key": "test_key", "api_secret": "test_secret"}

    def test_child_stream_always_accessible(self):
        """Child streams always return True from check_access regardless of API response."""
        from tap_yotpo.streams.product_reviews import ProductReviews

        stream = ProductReviews(self.mock_client)
        self.assertTrue(stream.check_access())
        self.mock_client.get.assert_not_called()

    def test_child_stream_product_variants_always_accessible(self):
        """product_variants (a child stream) always returns True."""
        from tap_yotpo.streams.product_variants import ProductVariants

        stream = ProductVariants(self.mock_client)
        self.assertTrue(stream.check_access())

    def test_parent_stream_accessible_when_api_returns_200(self):
        """Parent stream returns True when API call succeeds."""
        from tap_yotpo.streams.reviews import Reviews

        self.mock_client.get.return_value = {}
        stream = Reviews(self.mock_client)
        self.assertTrue(stream.check_access())

    def test_parent_stream_inaccessible_on_403(self):
        """Parent stream returns False when API raises Http403RequestError."""
        from tap_yotpo.exceptions import Http403RequestError
        from tap_yotpo.streams.reviews import Reviews

        self.mock_client.get.side_effect = Http403RequestError()
        stream = Reviews(self.mock_client)
        with patch("tap_yotpo.streams.abstracts.LOGGER") as mock_logger:
            self.assertFalse(stream.check_access())

        warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
        self.assertTrue(any("reviews" in c for c in warning_calls))


class TestDiscover(unittest.TestCase):
    """Tests for the discover() function."""

    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_client.config = {"api_key": "test_key", "api_secret": "test_secret"}

    def test_returns_catalog_with_all_streams_when_all_accessible(self):
        """discover() returns a Catalog with all streams when all are accessible."""

        def always_accessible(self_):
            return True

        with patch.object(BaseStream, "check_access", new=always_accessible):
            catalog = discover(self.mock_client)

        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(stream_ids, set(STREAMS.keys()))

    def test_excludes_inaccessible_streams_from_catalog(self):
        """discover() excludes streams that fail the access check."""

        def check_access_fn(self_):
            if getattr(self_, "parent", ""):
                return True
            return self_.tap_stream_id != "reviews"

        with patch.object(BaseStream, "check_access", new=check_access_fn):
            catalog = discover(self.mock_client)

        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn("reviews", stream_ids)
        self.assertIn("products", stream_ids)

    def test_catalog_streams_have_schema_and_metadata(self):
        """Each stream entry in the catalog has a schema and metadata."""

        def always_accessible(self_):
            return True

        with patch.object(BaseStream, "check_access", new=always_accessible):
            catalog = discover(self.mock_client)

        for stream_entry in catalog.streams:
            self.assertIsNotNone(stream_entry.schema)
            self.assertIsNotNone(stream_entry.metadata)
            self.assertGreater(len(stream_entry.metadata), 0)

    def test_catalog_stream_primary_keys_marked_automatic(self):
        """Primary key fields in stream metadata have inclusion set to 'automatic'."""

        def always_accessible(self_):
            return True

        with patch.object(BaseStream, "check_access", new=always_accessible):
            catalog = discover(self.mock_client)

        for stream_entry in catalog.streams:
            stream_class = STREAMS[stream_entry.tap_stream_id]
            metadata_map = {tuple(e["breadcrumb"]): e["metadata"] for e in stream_entry.metadata}
            for key in stream_class.key_properties:
                key_meta = metadata_map.get(("properties", key), {})
                self.assertEqual(
                    key_meta.get("inclusion"),
                    "automatic",
                    f"Expected 'automatic' for key '{key}' in stream '{stream_entry.tap_stream_id}'",
                )

    def test_catalog_stream_replication_keys_marked_automatic(self):
        """Replication key fields for incremental streams are marked as automatic."""

        def always_accessible(self_):
            return True

        with patch.object(BaseStream, "check_access", new=always_accessible):
            catalog = discover(self.mock_client)

        for stream_entry in catalog.streams:
            stream_class = STREAMS[stream_entry.tap_stream_id]
            if not stream_class.valid_replication_keys:
                continue
            metadata_map = {tuple(e["breadcrumb"]): e["metadata"] for e in stream_entry.metadata}
            for key in stream_class.valid_replication_keys:
                key_meta = metadata_map.get(("properties", key), {})
                self.assertEqual(
                    key_meta.get("inclusion"),
                    "automatic",
                    f"Expected 'automatic' for replication key '{key}' in stream '{stream_entry.tap_stream_id}'",
                )
