import requests
import singer
from singer import metrics
import backoff
from requests.exceptions import Timeout, ConnectionError

LOGGER = singer.get_logger()

AUTH_URL = "https://api.yotpo.com/oauth/token"
BASE_URL = "https://api.yotpo.com"
BASE_URL_V1 = "https://api-cdn.yotpo.com/v1"

GRANT_TYPE = "client_credentials"
REQUEST_TIMEOUT = 300

class YotpoError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class YotpoBadRequestError(YotpoError):
    pass


class YotpoRateLimitError(YotpoError):
    pass


class YotpoUnauthorizedError(YotpoError):
    pass


class YotpoForbiddenError(YotpoError):
    pass


class YotpoBadGateway(YotpoError):
    pass


class YotpoNotFoundError(YotpoError):
    pass


class YotpoTooManyError(YotpoRateLimitError):
    pass


class YotpoNotAvailableError(YotpoRateLimitError):
    pass


class YotpoGatewayTimeout(YotpoRateLimitError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": YotpoBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": YotpoUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": YotpoForbiddenError,
        "message": "User doesn't have permission to access the resource."
    },
    404: {
        "raise_exception": YotpoNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    429: {
        "raise_exception": YotpoTooManyError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    502: {
        "raise_exception": YotpoBadGateway,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": YotpoNotAvailableError,
        "message": "API service is currently unavailable."
    },
    504: {
        "raise_exception": YotpoGatewayTimeout,
        "message": "API service time out, please check Yotpo server."
    }
}


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


class Client(object):
    def __init__(self, config):
        self.user_agent = config.get("user_agent")
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
        if self.user_agent:
            request.headers["User-Agent"] = self.user_agent
        return self.session.send(self.session.prepare_request(request), timeout=self.request_timeout)

    def url(self, version, raw_path):
        path = raw_path \
            .replace(":api_key", self.api_key) \
            .replace(":token", self.token)

        if version == 'v1':
            return _join(BASE_URL_V1, path)
        else:
            return _join(BASE_URL, path)

    def create_get_request(self, version, path, **kwargs):
        return requests.Request(method="GET",
                                url=self.url(version, path),
                                **kwargs)

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
                          (YotpoRateLimitError, YotpoBadGateway,
                           YotpoUnauthorizedError),
                          max_tries=3,
                          factor=2)
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
            message = "HTTP-error-code: {}, Error: {}".format(
                response.status_code,
                response_json.get("status", ERROR_CODE_EXCEPTION_MAPPING.get(
                    response.status_code, {})).get("message", "Unknown Error")
            )
            exc = ERROR_CODE_EXCEPTION_MAPPING.get(
                response.status_code, {}).get("raise_exception", YotpoError)
            # Re-authenticating if got an authentication error while collecting stream data and response does not have Bad request (400), Forbidden (403) and Not found (404)
            if not authentication_call and response.status_code not in [400, 403, 404, 429]:
                self.authenticate()
            raise exc(message, response) from None
