import unittest 
from unittest import mock
from tap_yotpo.streams import transform
from tap_yotpo import Context, discover


class TestDataTransforming(unittest.TestCase):
    """
    To test that data can transformable to schema
    """
    def test_data_transform_for_reviews_stream(self):
        """
        To check that in review Stream' sku field not accept None value"""
    
        ctx = Context({}, {})
        ctx.catalog = discover(ctx)
        schema = ctx.catalog.get_stream('reviews').schema.to_dict()
        
        record = {'id': 255954628, 'title': 'test', 'content': 'test', 'score': 5, 'votes_up': 1, 'votes_down': 0, 'created_at': '2021-05-24T09:23:44.000Z', 'updated_at': '2021-05-24T11:37:25.000Z', 'sentiment': 0.976052, 'sku': None, 'name': 'test', 'email': 'test', 'reviewer_type': 'test', 'deleted': False, 'archived': False, 'escalated': False}
        transform_error = False
        
        try:
            transform(record, schema)
        except:
            transform_error = True
            
        self.assertEqual(transform_error, False, "Exception raised")

    """
    To test that data can transformable to schema.
    Checking that in product Stream, url field should accept not accept any value but String or None values
    """
    def test_data_transform_for_products_stream(self):
        class NonStringDataType(object):
            def __str__(self):
                raise TypeError('Can not by stringified')
        ctx = Context({}, {})
        ctx.catalog = discover(ctx)
        schema = ctx.catalog.get_stream('products').schema.to_dict()
        record = {"id": 37131285, "created_at": "2018-01-23T14:16:43.000000Z", "updated_at": "2021-05-24T09:23:43.000000Z", "average_score": 4.5, "total_reviews": 4.0, "external_product_id": "yotpo_site_reviews", "name": "https://stitchdatawearhouse.myshopify.com","url": NonStringDataType(), "description": "abcd", "product_specs": [], "category": {"id": 1, "name": "abcd"}, "images": []}

        transform_error = False
        
        try:
            result = transform(record, schema)
        except:
            transform_error = True
            
        self.assertEqual(transform_error, True, "Exception raised")

    """
    To test that data can transformable to schema.
    Checking that in product Stream, url field should accept None value
    """
    def test_data_transform_for_url_with_none_value_products_stream(self):
        ctx = Context({}, {})
        ctx.catalog = discover(ctx)
        schema = ctx.catalog.get_stream('products').schema.to_dict()
        record = {"id": 37131285, "created_at": "2018-01-23T14:16:43.000000Z", "updated_at": "2021-05-24T09:23:43.000000Z", "average_score": 4.5, "total_reviews": 4.0, "external_product_id": "yotpo_site_reviews", "name": "https://stitchdatawearhouse.myshopify.com","url": None, "description": "abcd", "product_specs": [], "category": {"id": 1, "name": "abcd"}, "images": []}

        transform_error = False
        
        try:
            result = transform(record, schema)
        except:
            transform_error = True
            
        self.assertEqual(transform_error, False, "Exception raised")