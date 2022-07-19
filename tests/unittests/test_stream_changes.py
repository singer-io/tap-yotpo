import unittest
from unittest import mock
from tap_yotpo import streams, context

import singer
LOGGER = singer.get_logger()

def mock_schema(a,b):
        return {}

def mock_records_with_empty_id(resp):
    return [{"unsubscirbed_by_name": "USER", "user_email": "abc@xyz.com", "email_type_id": 1.0, "id": ""}]

def mock_records_with_id(resp):
    return [{"unsubscirbed_by_name": "USER", "user_email": "abc@xyz.com", "email_type_id": 1.0, "id": 1}]

def mock_on_batch_complete(ctx, records,product_id=None):
    return False

class TestYotpoStreamChanges(unittest.TestCase):

    @mock.patch("tap_yotpo.metadata.to_map")
    @mock.patch("tap_yotpo.streams.Paginated.get_params", side_effect=mock_schema)
    @mock.patch("tap_yotpo.streams.Paginated.format_response", side_effect=mock_records_with_empty_id)
    @mock.patch("tap_yotpo.streams.Paginated.on_batch_complete", side_effect=mock_on_batch_complete)
    @mock.patch("tap_yotpo.streams.transform")
    def test_sync_of_paginated_class_with_unsubscribe_data_with_empty_id(self,mock_transform, mock_on_batch_complete,mock_records_with_empty_id,mock_get_params, mock_to_map):
        paginated = streams.Paginated("unsubscribers",["id"],[],"apps/:api_key/unsubscribers?utoken=:token",collection_key='unsubscribers')
        config = {}
        state = {}
        mock_to_map.return_value = {(): {'table-key-properties': ['id'], 'inclusion': 'available'}, ('properties', 'id'): {'inclusion': 'automatic'}, ('properties', 'created_at'): {'inclusion': 'available'}, ('properties', 'updated_at'): {'inclusion': 'available'}, ('properties', 'average_score'): {'inclusion': 'available'}, ('properties', 'total_reviews'): {'inclusion': 'available'}, ('properties', 'url'): {'inclusion': 'available'}, ('properties', 'external_product_id'): {'inclusion': 'available'}, ('properties', 'name'): {'inclusion': 'available'}, ('properties', 'description'): {'inclusion': 'available'}, ('properties', 'product_specs'): {'inclusion': 'available'}, ('properties', 'category'): {'inclusion': 'available'}, ('properties', 'images'): {'inclusion': 'available'}}
        mock_ctx = mock.Mock(context.Context(config,state))
        paginated._sync(mock_ctx)
        self.assertEqual(mock_transform.call_count,0)

    @mock.patch("tap_yotpo.metadata.to_map")
    @mock.patch("tap_yotpo.streams.Paginated.get_params", side_effect=mock_schema)
    @mock.patch("tap_yotpo.streams.Paginated.format_response", side_effect=mock_records_with_id)
    @mock.patch("tap_yotpo.streams.Paginated.on_batch_complete", side_effect=mock_on_batch_complete)
    @mock.patch("tap_yotpo.streams.transform")
    def test_sync_of_paginated_class_with_unsubscribe_data_with_id(self, mock_transform, mock_on_batch_complete, mock_records_with_id, mock_get_params, mock_to_map):
        paginated = streams.Paginated("unsubscribers",["id"],[],"apps/:api_key/unsubscribers?utoken=:token",collection_key='unsubscribers')
        config = {}
        state = {}
        mock_to_map.return_value = {(): {'table-key-properties': ['id'], 'inclusion': 'available'}, ('properties', 'id'): {'inclusion': 'automatic'}, ('properties', 'created_at'): {'inclusion': 'available'}, ('properties', 'updated_at'): {'inclusion': 'available'}, ('properties', 'average_score'): {'inclusion': 'available'}, ('properties', 'total_reviews'): {'inclusion': 'available'}, ('properties', 'url'): {'inclusion': 'available'}, ('properties', 'external_product_id'): {'inclusion': 'available'}, ('properties', 'name'): {'inclusion': 'available'}, ('properties', 'description'): {'inclusion': 'available'}, ('properties', 'product_specs'): {'inclusion': 'available'}, ('properties', 'category'): {'inclusion': 'available'}, ('properties', 'images'): {'inclusion': 'available'}}
        mock_ctx = mock.Mock(context.Context(config,state))
        paginated._sync(mock_ctx)
        self.assertEqual(mock_transform.call_count,1)