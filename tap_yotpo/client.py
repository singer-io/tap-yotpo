"""tap-yotpo client module."""
from typing import Any, Dict, Mapping, Optional, Tuple

import backoff
import requests
from requests import session
from singer import get_logger, metrics

from . import exceptions as errors
from .helpers import ApiSpec

LOGGER = get_logger()


def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception. Takes in a response object,
    checks the status code, and throws the associated exception based on the
    status code.

    :param resp: requests.Response object
    """
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as _:
        try:
            error_code = response.status_code
            client_exception = getattr(
                errors, f"Http{error_code}RequestError", errors.ClientError(message="Undefined Exception")
            )
            raise client_exception from None
        except (ValueError, TypeError, AttributeError):
            raise errors.ClientError(_) from None


class Client:
    """
    A Wrapper class with support for V1 & V3 and UGC Yotpo api.
    ~~~
    Performs:
     - Authentication
     - Response parsing
     - HTTP Error handling and retry
    """

    auth_url = "https://api.yotpo.com/oauth/token"

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._session = session()
        self.__utoken = None
        self.req_counter: metrics.Counter = None
        self.req_timer: metrics.Timer = None

    def _get_auth_token(self, force: Optional[bool] = False):
        if self.__utoken and not force:
            return self.__utoken
        data = {
            "client_id": self.config["api_key"],
            "client_secret": self.config["api_secret"],
            "grant_type": "client_credentials",
        }
        # directly using the session object bypassing the self.__make_request
        # as a authentication failure creates a infinite recursion loop
        resp = self._session.request("POST", self.auth_url, data=data)
        if resp.status_code == 200:
            response = resp.json()
            self.__utoken = response["access_token"]
            LOGGER.info("Authenticating successful with yotpo api")
            return self.__utoken
        else:
            LOGGER.info("Authenticating Failed, exiting")
            raise errors.Http401RequestError

    def authenticate(
        self, headers: Optional[dict], params: Optional[dict], api_auth_version: Any = ApiSpec.API_V3
    ) -> Tuple[Dict, Dict]:
        """Updates Headers and Params based on api version of the stream."""
        if api_auth_version == ApiSpec.API_V1:
            params.update({"utoken": self._get_auth_token()})
        elif api_auth_version == ApiSpec.API_V3:
            headers.update({"X-Yotpo-Token": self._get_auth_token()})
        return headers, params

    @backoff.on_exception(wait_gen=backoff.expo, exception=(errors.Http401RequestError,), jitter=None, max_tries=1)
    def get(self, endpoint: str, params: Dict, headers: Dict, api_auth_version: Any) -> Any:
        """Calls the make_request method with a prefixed method type `GET`"""
        headers, params = self.authenticate(headers, params, api_auth_version)
        return self.__make_request("GET", endpoint, headers=headers, params=params)

    def post(self, endpoint: str, params: Dict, headers: Dict, api_auth_version: Any, body: Dict) -> Any:
        """Calls the make_request method with a prefixed method type `POST`"""
        # pylint: disable=R0913
        headers, params = self.authenticate(headers, params, api_auth_version)
        self.__make_request("POST", endpoint, headers=headers, params=params, data=body)

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            errors.Http400RequestError,
            errors.Http404RequestError,
            errors.Http406RequestError,
            errors.Http500RequestError,
            errors.Http502RequestError,
            errors.Http503RequestError,
            errors.Http504RequestError,
        ),
        jitter=None,
        max_tries=5,
    )
    @backoff.on_exception(
        wait_gen=backoff.expo, exception=errors.Http429RequestError, jitter=None, max_time=60, max_tries=6
    )
    def __make_request(self, method: str, endpoint: str, **kwargs) -> Optional[Mapping[Any, Any]]:
        """
        Performs HTTP Operations
        Args:
            method (str): represents the state file for the tap.
            endpoint (str): url of the resource that needs to be fetched
            params (dict): A mapping for url params eg: ?name=Avery&age=3
            headers (dict): A mapping for the headers that need to be sent
            body (dict): only applicable to post request, body of the request

        Returns:
            Dict,List,None: Returns a `Json Parsed` HTTP Response or None if exception
        """
        response = self._session.request(method, endpoint, **kwargs)
        if self.req_counter:
            self.req_counter.increment()
        if response.status_code != 200:
            try:
                LOGGER.error("Failed due: %s", response.text)
            except AttributeError:
                pass
            try:
                raise_for_error(response)
            except errors.Http401RequestError as _:
                LOGGER.info("Authorization Failure, attempting to regenrate token")
                self._get_auth_token(force=True)
                raise _
            except errors.Http404RequestError as _:
                LOGGER.error("Resource Not Found %s", response.url or "")
                raise _
            return None
        return response.json()
