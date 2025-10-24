import unittest
from unittest.mock import Mock, patch
import os
import json

from tap_yotpo.streams.order_fulfillments import OrderFulfillments
from tap_yotpo.streams.product_reviews import ProductReviews
from tap_yotpo.streams.product_variants import ProductVariants
from tap_yotpo.streams.products import Products
from tap_yotpo.streams.orders import Orders
from tap_yotpo.client import Client


class TestParentMetadata(unittest.TestCase):
    """Test cases for parent-tap-stream-id metadata functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a mock client with config
        self.mock_client = Mock()
        self.mock_client.config = {"api_key": "test_key", "api_secret": "test_secret"}

    def _get_stream_metadata(self, metadata):
        """Helper to find stream-level metadata (breadcrumb = ())."""
        for entry in metadata:
            if entry['breadcrumb'] == ():
                return entry
        return None

    def test_parent_metadata_order_fulfillments(self):
        """Test that order_fulfillments stream has parent metadata set to 'orders'."""
        # Test the get_metadata method directly on the class
        # Load a simple schema for testing
        simple_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "updated_at": {"type": "string", "format": "date-time"}
            }
        }
        
        metadata = OrderFulfillments.get_metadata(simple_schema)
        stream_metadata = self._get_stream_metadata(metadata)
        
        # Assert that parent-tap-stream-id is set correctly
        self.assertIsNotNone(stream_metadata)
        self.assertEqual(stream_metadata['metadata']['parent-tap-stream-id'], 'orders')

    def test_parent_metadata_product_reviews(self):
        """Test that product_reviews stream has parent metadata set to 'products'."""
        # Test the get_metadata method directly on the class
        simple_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "created_at": {"type": "string", "format": "date-time"}
            }
        }
        
        metadata = ProductReviews.get_metadata(simple_schema)
        stream_metadata = self._get_stream_metadata(metadata)
        
        # Assert that parent-tap-stream-id is set correctly
        self.assertIsNotNone(stream_metadata)
        self.assertEqual(stream_metadata['metadata']['parent-tap-stream-id'], 'products')

    def test_parent_metadata_product_variants(self):
        """Test that product_variants stream has parent metadata set to 'products'."""
        # Test the get_metadata method directly on the class
        simple_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "updated_at": {"type": "string", "format": "date-time"}
            }
        }
        
        metadata = ProductVariants.get_metadata(simple_schema)
        stream_metadata = self._get_stream_metadata(metadata)
        
        # Assert that parent-tap-stream-id is set correctly
        self.assertIsNotNone(stream_metadata)
        self.assertEqual(stream_metadata['metadata']['parent-tap-stream-id'], 'products')

    def test_no_parent_metadata_for_parent_streams(self):
        """Test that parent streams (products, orders) do not have parent metadata."""
        # Test Products stream (parent stream)
        simple_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"}
            }
        }
        
        products_metadata = Products.get_metadata(simple_schema)
        products_stream_metadata = self._get_stream_metadata(products_metadata)
        
        # Assert that parent-tap-stream-id is NOT set for parent streams
        self.assertIsNotNone(products_stream_metadata)
        self.assertNotIn('parent-tap-stream-id', products_stream_metadata['metadata'])
        
        # Test Orders stream (parent stream)
        orders_metadata = Orders.get_metadata(simple_schema)
        orders_stream_metadata = self._get_stream_metadata(orders_metadata)
        
        # Assert that parent-tap-stream-id is NOT set for parent streams
        self.assertIsNotNone(orders_stream_metadata)
        self.assertNotIn('parent-tap-stream-id', orders_stream_metadata['metadata'])

    def test_parent_attribute_exists_on_child_streams(self):
        """Test that the parent class attribute exists on child streams."""
        self.assertTrue(hasattr(OrderFulfillments, 'parent'))
        self.assertEqual(OrderFulfillments.parent, 'orders')
        
        self.assertTrue(hasattr(ProductReviews, 'parent'))
        self.assertEqual(ProductReviews.parent, 'products')
        
        self.assertTrue(hasattr(ProductVariants, 'parent'))
        self.assertEqual(ProductVariants.parent, 'products')

    def test_parent_attribute_not_exists_on_parent_streams(self):
        """Test that the parent class attribute does not exist on parent streams."""
        # Products and Orders should not have parent attribute
        self.assertFalse(hasattr(Products, 'parent'))
        self.assertFalse(hasattr(Orders, 'parent'))
