import unittest
from unittest import mock
from tap_yotpo import streams, context

def mock_schema(a,b):
        return {}

def mock_records_with_empty_id(resp):
    return [{"unsubscirbed_by_name": "USER", "user_email": "abc@xyz.com", "email_type_id": 1.0, "id": ""}]

def mock_records_with_id(resp):
    return [{"unsubscirbed_by_name": "USER", "user_email": "abc@xyz.com", "email_type_id": 1.0, "id": 1}]

class TestYotpoStreamChanges(unittest.TestCase):

    @mock.patch("tap_yotpo.streams.Paginated.get_params", side_effect=mock_schema)
    @mock.patch("tap_yotpo.streams.Paginated.format_response", side_effect=mock_records_with_id)
    @mock.patch("singer.transform")
    def test_sync_of_paginated_class_with_unsubscribe_data_with_empty_id(self,mock_get_params,mock_records_with_empty_id,mock_transform):
        paginated = streams.Paginated("unsubscribers",["id"],"apps/:api_key/unsubscribers?utoken=:token",collection_key='unsubscribers')
        config = {}
        state = {}
        mock_ctx = mock.Mock(context.Context(config,state))
        paginated._sync(mock_ctx)
        self.assertEqual(mock_transform.call_count,0)
