import enum
import unittest 
from unittest import mock
from singer.transform import SchemaMismatch
import singer
from tap_yotpo.client import Client
from tap_yotpo.discover import discover
from singer import Transformer


class TestDataTransforming(unittest.TestCase):
    """
    To test that data can transformable to schema
    """
    @mock.patch("tap_yotpo.client.Client.authenticate")
    def test_data_transform_for_id_stream(self, mock_client_auth):
        """
        To check that in review Stream id field should not accept None value
        """
        client_obj = Client({"api_key": enum.auto(), "api_secret": enum.auto()})
        catalog = discover(client_obj)

        streams = catalog.get_selected_streams(state= {})

        for stream in streams :
            if stream == 'reviews' :
                stream_schema = stream.schema.to_dict()
                stream_metadata = singer.metadata.to_map(stream.metadata)

        record = {'id': None, 'title': 'test', 'content': 'test', 'score': 5, 'votes_up': 1, 'votes_down': 0, 'created_at': '2021-05-24T09:23:44.000Z', 'updated_at': '2021-05-24T11:37:25.000Z', 'sentiment': 0.976052, 'sku': None, 'name': 'test', 'email': 'test', 'reviewer_type': 'test', 'deleted': False, 'archived': False, 'escalated': False}
        with self.assertRaises(SchemaMismatch):
            Transformer.transform(record, stream_schema,stream_metadata)

    # @mock.patch("tap_yotpo.streams.sync")
    # def test_scientific_notation_in_product_id(self,mock_sync):
    #     """
    #     If any scientific notation product-id found, then log the warning and process for next product-id.
    #     """
    #     class MockContext():
    #         cache = {
    #             "products" : [{"external_product_id": "4.76625E+12"}, {"external_product_id": "123456789"}]
    #         }
    #     product_reviews = ProductReviews("product_reviews", ["id"], ["created_at"], "widget/:api_key/products/{product_id}/reviews.json", collection_key="reviews", version='v1')
    #     mock_ctx = MockContext()
    #     product_reviews.sync(mock_ctx)
    #     self.assertEqual(mock_sync.call_count,1)