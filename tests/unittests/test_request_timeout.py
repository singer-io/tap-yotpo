from tap_yotpo.client import Client
import unittest
from unittest import mock
from unittest.case import TestCase
from requests.exceptions import Timeout, ConnectionError
import datetime
from tap_yotpo import LOGGER

class TestBackoffError(unittest.TestCase):
    '''
    Test that backoff logic works properly.
    '''
    @mock.patch('tap_yotpo.client.requests.Session.send')
    @mock.patch('tap_yotpo.client.requests.Session.prepare_request')
    def test_backoff_prepare_and_send_timeout_error(self, mock_prepare_request, mock_send):
        """
        Check whether the request backoffs properly for 5 times in case of Timeout error.
        """
        mock_send.side_effect = Timeout
        with self.assertRaises(Timeout):
            config = {"start_date": "dummy_st", "api_key": "dummy_key", "api_secret": "dummy_secret"}
            client = Client(config)
            client.prepare_and_send("request")
        self.assertEqual(mock_send.call_count, 5)

    @mock.patch('tap_yotpo.client.requests.Session.send')
    @mock.patch('tap_yotpo.client.requests.Session.prepare_request')
    def test_backoff_prepare_and_send_connection_error(self, mock_prepare_request, mock_send):
        """
        Check whether the request backoffs properly for 5 times in case of Timeout error.
        """
        mock_send.side_effect = ConnectionError
        with self.assertRaises(ConnectionError):
            config = {"start_date": "dummy_st", "api_key": "dummy_key", "api_secret": "dummy_secret"}
            client = Client(config)
            client.prepare_and_send("request")
        self.assertEqual(mock_send.call_count, 5)

class MockResponse():
    '''
    Mock response  object for the requests call 
    '''
    def __init__(self, resp, status_code, content=[""], headers=None, raise_error=False, text={}):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"

    def prepare(self):
        return (self.json_data, self.status_code, self.content, self.headers, self.raise_error)

    def json(self):
        return self.text

class TestRequestTimeoutValue(unittest.TestCase):
    '''
    Test that request timeout parameter works properly in various cases
    '''
    @mock.patch('tap_yotpo.client.requests.Session.send', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_yotpo.client.requests.Session.prepare_request', return_value = "request")
    def test_config_provided_request_timeout(self, mock_request, mock_send):
        """ 
            Unit tests to ensure that request timeout is set based on config value
        """
        config = {"start_date": "dummy_st", "api_key": "dummy_key", "api_secret": "dummy_secret", "request_timeout": 100}
        client = Client(config)
        client.prepare_and_send("request")
        mock_send.assert_called_with("request", timeout=100.0)

    @mock.patch('tap_yotpo.client.requests.Session.send', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_yotpo.client.requests.Session.prepare_request', return_value = "request")
    def test_default_value_request_timeout(self, mock_request, mock_send):
        """ 
            Unit tests to ensure that request timeout is set based default value
        """
        config = {"start_date": "dummy_st", "api_key": "dummy_key", "api_secret": "dummy_secret"}
        client = Client(config)
        client.prepare_and_send("request")
        mock_send.assert_called_with("request", timeout=300.0)

    @mock.patch('tap_yotpo.client.requests.Session.send', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_yotpo.client.requests.Session.prepare_request', return_value = "request")
    def test_config_provided_empty_request_timeout(self, mock_request, mock_send):
        """ 
            Unit tests to ensure that request timeout is set based on default value if empty value is given in config
        """
        config = {"start_date": "dummy_st", "api_key": "dummy_key", "api_secret": "dummy_secret", "request_timeout": ""}
        client = Client(config)
        client.prepare_and_send("request")
        mock_send.assert_called_with("request", timeout=300.0)
        
    @mock.patch('tap_yotpo.client.requests.Session.send', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_yotpo.client.requests.Session.prepare_request', return_value = "request")
    def test_config_provided_string_request_timeout(self, mock_request, mock_send):
        """ 
            Unit tests to ensure that request timeout is set based on config string value
        """
        config = {"start_date": "dummy_st", "api_key": "dummy_key", "api_secret": "dummy_secret", "request_timeout": "100"}
        client = Client(config)
        client.prepare_and_send("request")
        mock_send.assert_called_with("request", timeout=100.0)

    @mock.patch('tap_yotpo.client.requests.Session.send', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_yotpo.client.requests.Session.prepare_request', return_value = "request")
    def test_config_provided_float_request_timeout(self, mock_request, mock_send):
        """ 
            Unit tests to ensure that request timeout is set based on config float value
        """
        config = {"start_date": "dummy_st", "api_key": "dummy_key", "api_secret": "dummy_secret", "request_timeout": 100.8}
        client = Client(config)
        client.prepare_and_send("request")
        mock_send.assert_called_with("request", timeout=100.8)