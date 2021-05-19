import unittest
from unittest import mock
from tap_yotpo import http

# mock responce
class Mockresponse:
        def __init__(self, resp, status_code, content=[], headers=None, raise_error=False):
            self.json_data = resp
            self.status_code = status_code
            self.content = content
            self.headers = headers
            self.raise_error = raise_error

        def prepare(self):
            return (self.json_data, self.status_code, self.content, self.headers, self.raise_error)

class TestYotpoErrorHandling(unittest.TestCase):

    def mock_prepare_and_send(request):
        return Mockresponse("",404)

    @mock.patch("tap_yotpo.http.Client.prepare_and_send",side_effect=mock_prepare_and_send)
    def test_request_with_handling_for_404_exceptin_handling(self,mock_prepare_and_send):
        try:
            request = None
            tap_stream_id = "tap_yopto"
            mock_config = {"api_key":"mock_key","api_secret":"mock_secret"}
            mock_client = http.Client(mock_config)
            mock_client.request_with_handling(request,tap_stream_id)
        except http.NotFoundException:
            pass