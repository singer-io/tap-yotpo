import unittest 
from unittest import mock
from tap_yotpo.streams import transform, ProductReviews
from tap_yotpo import Context, discover
from singer.transform import SchemaMismatch


class TestDataTransforming(unittest.TestCase):
    """
    To test that data can transformable to schema
    """
    def test_data_transform_for_id_stream(self):
        """
        To check that in review Stream id field should not accept None value
        """
        ctx = Context({}, {})
        ctx.catalog = discover(ctx)
        schema = ctx.catalog.get_stream('reviews').schema.to_dict()
        
        record = {'id': None, 'title': 'test', 'content': 'test', 'score': 5, 'votes_up': 1, 'votes_down': 0, 'created_at': '2021-05-24T09:23:44.000Z', 'updated_at': '2021-05-24T11:37:25.000Z', 'sentiment': 0.976052, 'sku': None, 'name': 'test', 'email': 'test', 'reviewer_type': 'test', 'deleted': False, 'archived': False, 'escalated': False}
        with self.assertRaises(SchemaMismatch):
            transform(record, schema)

    @mock.patch("tap_yotpo.streams.Paginated._sync")
    def test_scientific_notation_in_product_id(self,mock_sync):
        """
        If any scientific notation product-id found, then log the warning and process for next product-id.
        """
        class MockContext():
            cache = {
                "products" : [{"external_product_id": "4.76625E+12"}, {"external_product_id": "123456789"}]
            }
        product_reviews = ProductReviews("product_reviews", ["id"], ["created_at"], "widget/:api_key/products/{product_id}/reviews.json", collection_key="reviews", version='v1')
        mock_ctx = MockContext()
        product_reviews.sync(mock_ctx)
        self.assertEqual(mock_sync.call_count,1)

    def test_data_transform_for_url_with_none_value_products_stream(self):
        """
        To test that data can transformable to schema.
        Checking that in product Stream, url field should accept None value
        """
        ctx = Context({}, {})
        ctx.catalog = discover(ctx)
        schema = ctx.catalog.get_stream('products').schema.to_dict()
        record = {"id": 37131285, "created_at": "2018-01-23T14:16:43.000000Z", "updated_at": "2021-05-24T09:23:43.000000Z", "average_score": 4.5, "total_reviews": 4.0, "external_product_id": "yotpo_site_reviews", "name": "https://stitchdatawearhouse.myshopify.com","url": None, "description": "abcd", "product_specs": [], "category": {"id": 1, "name": "abcd"}, "images": []}
        
        result = transform(record, schema)

        self.assertEqual(record,result)