"""Unit tests for tap-yotpo discovery module."""
from unittest import TestCase
from unittest.mock import MagicMock, patch

from tap_yotpo.discover import _apply_access_checks, _prune_inaccessible_children
from tap_yotpo.exceptions import Http403RequestError
from tap_yotpo.streams import STREAMS
from tap_yotpo.streams.products import Products
from tap_yotpo.streams.reviews import Reviews
from tap_yotpo.streams.product_reviews import ProductReviews


class TestCheckAccess(TestCase):
    """Tests for BaseStream.check_access()."""

    def _make_client(self):
        client = MagicMock()
        client.config = {"api_key": "test_key", "api_secret": "test_secret"}
        return client

    def test_child_stream_always_returns_true(self):
        """check_access() must return True for child streams without calling the API."""
        client = self._make_client()
        stream = ProductReviews(client=client)
        self.assertTrue(stream.check_access())
        client.get.assert_not_called()

    def test_parent_stream_returns_true_on_success(self):
        """check_access() returns True when the API call succeeds."""
        client = self._make_client()
        client.get.return_value = {}
        stream = Reviews(client=client)
        self.assertTrue(stream.check_access())

    def test_parent_stream_returns_false_on_403(self):
        """check_access() returns False when a Http403RequestError is raised."""
        client = self._make_client()
        client.get.side_effect = Http403RequestError()
        stream = Reviews(client=client)
        self.assertFalse(stream.check_access())


class TestPruneInaccessibleChildren(TestCase):
    """Tests for _prune_inaccessible_children()."""

    def _sample_schemas(self):
        return {name: {} for name in STREAMS}

    def test_child_stays_when_parent_present(self):
        schemas = self._sample_schemas()
        field_metadata = self._sample_schemas()
        _prune_inaccessible_children(schemas, field_metadata)
        self.assertIn("product_reviews", schemas)

    def test_child_removed_when_parent_absent(self):
        schemas = self._sample_schemas()
        field_metadata = self._sample_schemas()
        # remove parent
        schemas.pop("products")
        field_metadata.pop("products")
        _prune_inaccessible_children(schemas, field_metadata)
        self.assertNotIn("product_reviews", schemas)
        self.assertNotIn("product_variants", schemas)

    def test_order_fulfillments_removed_when_orders_absent(self):
        schemas = self._sample_schemas()
        field_metadata = self._sample_schemas()
        schemas.pop("orders")
        field_metadata.pop("orders")
        _prune_inaccessible_children(schemas, field_metadata)
        self.assertNotIn("order_fulfillments", schemas)


class TestApplyAccessChecks(TestCase):
    """Tests for _apply_access_checks()."""

    def _make_client(self):
        client = MagicMock()
        client.config = {"api_key": "test_key", "api_secret": "test_secret"}
        return client

    def _sample_dicts(self):
        schemas = {name: {} for name in STREAMS}
        field_metadata = {name: [] for name in STREAMS}
        return schemas, field_metadata

    def test_all_accessible_schemas_unchanged(self):
        """All streams accessible: schemas dict must be unchanged."""
        client = self._make_client()
        client.get.return_value = {}
        schemas, field_metadata = self._sample_dicts()
        original_keys = set(schemas.keys())
        _apply_access_checks(client, schemas, field_metadata)
        self.assertEqual(original_keys, set(schemas.keys()))

    def test_inaccessible_stream_excluded(self):
        """A single inaccessible parent stream is removed from schemas."""
        client = self._make_client()

        def side_effect(url, *args, **kwargs):
            if "reviews" in url and "products" not in url:
                raise Http403RequestError()
            return {}

        client.get.side_effect = side_effect
        schemas, field_metadata = self._sample_dicts()
        _apply_access_checks(client, schemas, field_metadata)
        self.assertNotIn("reviews", schemas)
        self.assertNotIn("reviews", field_metadata)

    def test_all_inaccessible_raises(self):
        """All streams inaccessible: Http403RequestError must be raised."""
        client = self._make_client()
        client.get.side_effect = Http403RequestError()
        schemas, field_metadata = self._sample_dicts()
        with self.assertRaises(Http403RequestError):
            _apply_access_checks(client, schemas, field_metadata)

    def test_child_excluded_with_parent(self):
        """Child stream is removed when its parent is inaccessible."""
        client = self._make_client()

        def side_effect(url, *args, **kwargs):
            if "products" in url and "product" not in url.split("/stores/test_key/")[-1].split("/")[0]:
                # This hits the products stream endpoint
                pass
            # block products stream
            if url == Products(client=client).get_url_endpoint():
                raise Http403RequestError()
            return {}

        client.get.side_effect = side_effect
        schemas, field_metadata = self._sample_dicts()
        # Manually remove products to simulate inaccessible parent, then test pruning
        schemas.pop("products")
        field_metadata.pop("products")
        _prune_inaccessible_children(schemas, field_metadata)
        self.assertNotIn("product_reviews", schemas)
        self.assertNotIn("product_variants", schemas)
