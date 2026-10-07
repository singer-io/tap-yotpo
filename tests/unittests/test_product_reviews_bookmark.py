import unittest
from unittest.mock import Mock, patch

from tap_yotpo.streams.product_reviews import ProductReviews


class TestProductReviewsBookmarkResume(unittest.TestCase):
    def setUp(self):
        self.mock_client = Mock()
        self.mock_client.config = {
            "api_key": "test_key",
            "api_secret": "test_secret",
            "start_date": "2024-01-01T00:00:00Z",
        }

    @patch("tap_yotpo.streams.product_reviews.Products.prefetch_product_ids")
    def test_get_products_resumes_when_currently_syncing_is_string_and_product_id_is_integer(self, mock_prefetch):
        mock_prefetch.return_value = [(101, "ext-101"), (102, "ext-102"), (103, "ext-103")]
        stream = ProductReviews(self.mock_client)

        state = {
            "bookmarks": {
                "product_reviews": {
                    "currently_syncing": "102",
                }
            }
        }

        _products, start_index = stream.get_products(state)

        self.assertEqual(start_index, 1)

    @patch("tap_yotpo.streams.product_reviews.Products.prefetch_product_ids")
    def test_get_products_defaults_to_zero_when_currently_syncing_not_found(self, mock_prefetch):
        mock_prefetch.return_value = [(101, "ext-101"), (102, "ext-102")]
        stream = ProductReviews(self.mock_client)

        state = {
            "bookmarks": {
                "product_reviews": {
                    "currently_syncing": "999",
                }
            }
        }

        _products, start_index = stream.get_products(state)

        self.assertEqual(start_index, 0)
