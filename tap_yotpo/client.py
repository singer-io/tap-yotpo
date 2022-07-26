import requests
import singer
from singer import metrics
import backoff
from requests.exceptions import Timeout, ConnectionError
from exceptions import (YotpoRateLimitError, YotpoServer5xxError,
                       YotpoUnauthorizedError, YotpoError,
                       ERROR_CODE_EXCEPTION_MAPPING)
from helpers import _join

LOGGER = singer.get_logger()




class Client(object):
    
    AUTH_URL = "https://api.yotpo.com/oauth/token"
    BASE_URL = "https://api.yotpo.com"
    BASE_URL_V1 = "https://api-cdn.yotpo.com/v1"
    GRANT_TYPE = "client_credentials"
    REQUEST_TIMEOUT = 300

    def __init__(self, config):
        ###self.user_agent = config.get("user_agent")
        self.api_key = config.get("api_key")
        self.api_secret = config.get("api_secret")
        self.session = requests.Session()
        self.base_url = BASE_URL
        self._token = None
        request_timeout = config.get('request_timeout')
        # if request_timeout is other than 0,"0" or "" then use request_timeout
        if request_timeout and float(request_timeout):
            request_timeout = float(request_timeout)
        else: # If value is 0,"0" or "" then set default to 300 seconds.
            request_timeout = REQUEST_TIMEOUT
        self.request_timeout = request_timeout

    @property
    def token(self):
        if self._token is None:
            raise RuntimeError("Client is not yet authenticated")
        return self._token

    # Backoff for 5 times when a Timeout or Connection error occurs when sending the request
    @backoff.on_exception(backoff.expo, (Timeout, ConnectionError), max_tries=5, factor=2)
    def prepare_and_send(self, request):
        ### if self.user_agent:
        ###     request.headers["User-Agent"] = self.user_agent
        return self.session.send(self.session.prepare_request(request), timeout=self.request_timeout)

    def url(self, version, raw_path):
        path = raw_path.replace(":api_key", self.api_key).replace(":token", self.token)

        if version == 'v1':
            return _join(BASE_URL_V1, path)
        else:
            return _join(BASE_URL, path)

    def create_get_request(self, version, path, **kwargs):
        return requests.Request(method="GET",url=self.url(version, path),**kwargs)

    def request_with_handling(self, request, tap_stream_id):
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.prepare_and_send(request)
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        self.check_status(response)
        return response.json()

    def authenticate(self):
        auth_body = {
            "client_id": self.api_key,
            "client_secret": self.api_secret,
            "grant_type": GRANT_TYPE
        }
        request = requests.Request(method="POST", url=AUTH_URL, data=auth_body)
        response = self.prepare_and_send(request)
        self.check_status(response, authentication_call=True)
        data = response.json()
        self._token = data['access_token']

    @backoff.on_exception(backoff.expo,
                         (YotpoRateLimitError, YotpoServer5xxError,YotpoUnauthorizedError),
                         max_tries=3, factor=2)
    def GET(self, version, request_kwargs, *args, **kwargs):
        req = self.create_get_request(version, **request_kwargs)
        return self.request_with_handling(req, *args, **kwargs)

    def check_status(self, response, authentication_call=False):
        # Forming a response message for raising custom exception
        try:
            response_json = response.json()
        except Exception:
            response_json = {}
        if response.status_code != 200:
            message = "HTTP-error-code: {}, Error: {}".format(response.status_code,response_json.get("status",
                                                             ERROR_CODE_EXCEPTION_MAPPING.get(
                                                             response.status_code, {})).get("message", response.text))
            exc = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get("raise_exception", YotpoError)
            """
            Re-authenticating if got an authentication error while collecting stream data and 
            response does not have Bad request (400), Forbidden (403) and Not found (404)
            """
            if not authentication_call and response.status_code not in [400, 403, 404, 429]:
                self.authenticate()
            raise exc(message, response) from None
