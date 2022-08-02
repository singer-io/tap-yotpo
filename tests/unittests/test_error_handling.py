import unittest
from unittest import mock
import requests
from tap_yotpo import client, exceptions

# mock responce
class Mockresponse:
        def __init__(self, resp, status_code, content=[], headers=None, raise_error=False):
            self.json_data = resp
            self.status_code = status_code
            self.content = content
            self.headers = headers
            self.raise_error = raise_error
            self.text = {}

        def prepare(self):
            return (self.json_data, self.status_code, self.content, self.headers, self.raise_error)

        def raise_for_status(self):
            if not self.raise_error:
                return self.status_code

            raise requests.HTTPError("mock sample message")

        def json(self):
            return self.text

class TestYotpoErrorHandling(unittest.TestCase):

    def mock_prepare_and_send_400(request):
        return Mockresponse("",400,raise_error=True)

    def mock_prepare_and_send_401(request):
        return Mockresponse("",401,raise_error=True)

    def mock_prepare_and_send_403(request):
        return Mockresponse("",403,raise_error=True)

    def mock_prepare_and_send_404(request):
        return Mockresponse("",404,raise_error=True)

    def mock_prepare_and_send_429(request):
        return Mockresponse("",429,raise_error=True)

    def mock_prepare_and_send_500(request):
        return Mockresponse("", 500, raise_error=True)

    def mock_prepare_and_send_502(request):
        return Mockresponse("",502,raise_error=True)

    def mock_prepare_and_send_503(request):
        return Mockresponse("",503,raise_error=True)

    def mock_prepare_and_send_504(request):
        return Mockresponse("",504,raise_error=True)

    def mock_prepare_and_send_505(request):
        return Mockresponse("",505,raise_error=True)

    @mock.patch("tap_yotpo.client.Client.prepare_and_send",side_effect=mock_prepare_and_send_400)
    @mock.patch("tap_yotpo.client.Client.create_get_request")
    def test_GET_for_400_exceptin_handling(self,mock_create_get_request,mock_prepare_and_send):
        try:
            tap_stream_id = "tap_yopto"
            mock_config = {"api_key":"mock_key","api_secret":"mock_secret"}
            mock_client = client.Client(mock_config)
            mock_client.GET('v1',{"path": "apps/:api_key/products?utoken=:token", "params": {"count": 1,"page": 1}},tap_stream_id)
        except exceptions.YotpoBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_prepare_and_send.call_count,1)
            self.assertEquals(mock_create_get_request.call_count,1)

    @mock.patch("tap_yotpo.client.Client.prepare_and_send",side_effect=mock_prepare_and_send_401)
    @mock.patch("tap_yotpo.client.Client.authenticate")
    @mock.patch("tap_yotpo.client.Client.create_get_request")
    def test_GET_for_401_exceptin_handling(self,mock_create_get_request,mock_authenticate,mock_prepare_and_send):
        try:
            tap_stream_id = "tap_yopto"
            mock_config = {"api_key":"mock_key","api_secret":"mock_secret"}
            mock_client = client.Client(mock_config)
            mock_client.GET('v1',{"path": "apps/:api_key/products?utoken=:token", "params": {"count": 1,"page": 1}},tap_stream_id)
        except exceptions.YotpoUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_prepare_and_send.call_count,3)
            self.assertEquals(mock_authenticate.call_count,3)
            self.assertEquals(mock_create_get_request.call_count,mock_authenticate.call_count)

    @mock.patch("tap_yotpo.client.Client.prepare_and_send",side_effect=mock_prepare_and_send_401)
    def test_authenticate_not_called_again_for_401(self,mock_prepare_and_send):
        try:
            mock_config = {"api_key":"mock_key","api_secret":"mock_secret"}
            mock_client = client.Client(mock_config)
            mock_client.authenticate()
        except exceptions.YotpoUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_prepare_and_send.call_count,1)
    
    @mock.patch("tap_yotpo.client.Client.prepare_and_send",side_effect=mock_prepare_and_send_403)
    @mock.patch("tap_yotpo.client.Client.create_get_request")
    def test_GET_for_403_exceptin_handling(self,mock_create_get_request,mock_prepare_and_send):
        try:
            tap_stream_id = "tap_yopto"
            mock_config = {"api_key":"mock_key","api_secret":"mock_secret"}
            mock_client = client.Client(mock_config)
            mock_client.GET('v1',{"path": "apps/:api_key/products?utoken=:token", "params": {"count": 1,"page": 1}},tap_stream_id)
        except exceptions.YotpoForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: User doesn't have permission to access the resource."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_prepare_and_send.call_count,1)
            self.assertEquals(mock_create_get_request.call_count,1)

    @mock.patch("tap_yotpo.client.Client.prepare_and_send",side_effect=mock_prepare_and_send_404)
    @mock.patch("tap_yotpo.client.Client.create_get_request")
    def test_GET_for_404_exceptin_handling(self,mock_create_get_request,mock_prepare_and_send):
        try:
            tap_stream_id = "tap_yopto"
            mock_config = {"api_key":"mock_key","api_secret":"mock_secret"}
            mock_client = client.Client(mock_config)
            mock_client.GET('v1',{"path": "apps/:api_key/products?utoken=:token", "params": {"count": 1,"page": 1}},tap_stream_id)
        except exceptions.YotpoNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_prepare_and_send.call_count,1)
            self.assertEquals(mock_create_get_request.call_count,1)

    @mock.patch("tap_yotpo.client.Client.prepare_and_send",side_effect=mock_prepare_and_send_429)
    @mock.patch("tap_yotpo.client.Client.create_get_request")
    def test_GET_for_429_exceptin_handling(self,mock_create_get_request,mock_prepare_and_send):
        try:
            tap_stream_id = "tap_yopto"
            mock_config = {"api_key":"mock_key","api_secret":"mock_secret"}
            mock_client = client.Client(mock_config)
            mock_client.GET('v1',{"path": "apps/:api_key/products?utoken=:token", "params": {"count": 1,"page": 1}},tap_stream_id)
        except exceptions.YotpoTooManyError as e:
            expected_error_message = "HTTP-error-code: 429, Error: The API rate limit for your organisation/application pairing has been exceeded."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_prepare_and_send.call_count,3)
            self.assertEquals(mock_create_get_request.call_count,3)

    @mock.patch("tap_yotpo.client.Client.prepare_and_send",side_effect=mock_prepare_and_send_500)
    @mock.patch("tap_yotpo.client.Client.authenticate")
    @mock.patch("tap_yotpo.client.Client.create_get_request")
    def test_GET_for_500_exceptin_handling(self,mock_create_get_request,mock_authenticate,mock_prepare_and_send):
        """To verify that when response get 500 status code then showing proper error message"""
        try:
            tap_stream_id = "tap_yopto"
            mock_config = {"api_key":"mock_key","api_secret":"mock_secret"}
            mock_client = client.Client(mock_config)
            mock_client.GET('v1',{"path": "apps/:api_key/products?utoken=:token", "params": {"count": 1,"page": 1}},tap_stream_id)
        except exceptions.YotpoInternalServerError as e:
            expected_error_message = "HTTP-error-code: 500, Error: An error has occurred at Yotpo's end."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_prepare_and_send.call_count,3)
            self.assertEquals(mock_authenticate.call_count,3)
            self.assertEquals(mock_create_get_request.call_count,mock_authenticate.call_count)

    @mock.patch("tap_yotpo.client.Client.prepare_and_send",side_effect=mock_prepare_and_send_502)
    @mock.patch("tap_yotpo.client.Client.authenticate")
    @mock.patch("tap_yotpo.client.Client.create_get_request")
    def test_GET_for_502_exceptin_handling(self,mock_create_get_request,mock_authenticate,mock_prepare_and_send):
        try:
            tap_stream_id = "tap_yopto"
            mock_config = {"api_key":"mock_key","api_secret":"mock_secret"}
            mock_client = client.Client(mock_config)
            mock_client.GET('v1',{"path": "apps/:api_key/products?utoken=:token", "params": {"count": 1,"page": 1}},tap_stream_id)
        except exceptions.YotpoBadGateway as e:
            expected_error_message = "HTTP-error-code: 502, Error: Server received an invalid response."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_prepare_and_send.call_count,3)
            self.assertEquals(mock_authenticate.call_count,3)
            self.assertEquals(mock_create_get_request.call_count,mock_authenticate.call_count)

    @mock.patch("tap_yotpo.client.Client.prepare_and_send",side_effect=mock_prepare_and_send_503)
    @mock.patch("tap_yotpo.client.Client.authenticate")
    @mock.patch("tap_yotpo.client.Client.create_get_request")
    def test_GET_for_503_exceptin_handling(self,mock_create_get_request,mock_authenticate,mock_prepare_and_send):
        try:
            tap_stream_id = "tap_yopto"
            mock_config = {"api_key":"mock_key","api_secret":"mock_secret"}
            mock_client = client.Client(mock_config)
            mock_client.GET('v1',{"path": "apps/:api_key/products?utoken=:token", "params": {"count": 1,"page": 1}},tap_stream_id)
        except exceptions.YotpoNotAvailableError as e:
            expected_error_message = "HTTP-error-code: 503, Error: API service is currently unavailable."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_prepare_and_send.call_count,3)
            self.assertEquals(mock_authenticate.call_count,3)
            self.assertEquals(mock_create_get_request.call_count,mock_authenticate.call_count)

    @mock.patch("tap_yotpo.client.Client.prepare_and_send",side_effect=mock_prepare_and_send_504)
    @mock.patch("tap_yotpo.client.Client.authenticate")
    @mock.patch("tap_yotpo.client.Client.create_get_request")
    def test_GET_for_504_exceptin_handling(self,mock_create_get_request,mock_authenticate,mock_prepare_and_send):
        try:
            tap_stream_id = "tap_yopto"
            mock_config = {"api_key":"mock_key","api_secret":"mock_secret"}
            mock_client = client.Client(mock_config)
            mock_client.GET('v1',{"path": "apps/:api_key/products?utoken=:token", "params": {"count": 1,"page": 1}},tap_stream_id)
        except exceptions.YotpoGatewayTimeout as e:
            expected_error_message = "HTTP-error-code: 504, Error: API service time out, please check Yotpo server."
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_prepare_and_send.call_count,3)
            self.assertEquals(mock_authenticate.call_count,3)
            self.assertEquals(mock_create_get_request.call_count,mock_authenticate.call_count)

    @mock.patch("tap_yotpo.client.Client.prepare_and_send",side_effect=mock_prepare_and_send_505)
    @mock.patch("tap_yotpo.client.Client.authenticate")
    @mock.patch("tap_yotpo.client.Client.create_get_request")
    def test_GET_for_505_exceptin_handling(self,mock_create_get_request,mock_authenticate,mock_prepare_and_send):
        try:
            tap_stream_id = "tap_yopto"
            mock_config = {"api_key":"mock_key","api_secret":"mock_secret"}
            mock_client = client.Client(mock_config)
            mock_client.GET('v1',{"path": "apps/:api_key/products?utoken=:token", "params": {"count": 1,"page": 1}},tap_stream_id)
        except exceptions.YotpoError as e:
            expected_error_message = "HTTP-error-code: 505, Error: {}"
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            self.assertEquals(mock_prepare_and_send.call_count,1)
            self.assertEquals(mock_authenticate.call_count,1)
            self.assertEquals(mock_create_get_request.call_count,mock_authenticate.call_count)