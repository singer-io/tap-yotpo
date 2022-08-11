from typing import Any
from requests import session
import singer
import backoff
import requests
from . import exceptions as errors
LOGGER = singer.get_logger()

def raise_for_error(response: requests.Response):
    """
    Raises the associated response exception.
    Takes in a response object, checks the status code, and throws the associated
    exception based on the status code.
    :param resp: requests.Response object
    """
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as _:
        try:
            error_code = response.status_code
            client_exception = getattr(errors,f"Http{error_code}RequestError",errors.ClientError(message="Undefined Exception"))
            raise client_exception from None
        except (ValueError, TypeError,AttributeError):
            raise errors.ClientError(_) from None

class Client:
 
    auth_url = "https://api.yotpo.com/oauth/token"
 
    def __init__(self,config) -> None:
        self.config = config
        self._session = session()
        self.__utoken = None
        self._get_auth_token(regenerate_token=True)

    def _get_auth_token(self,regenerate_token=False):
        if self.__utoken and not regenerate_token:
            return self.__utoken
        
        data = {
            "client_id": self.config["api_key"],
            "client_secret": self.config["api_secret"],
            "grant_type": "client_credentials"
        }
        response = self.make_request("POST",self.auth_url,data=data)
        self.__utoken = response['access_token']
        LOGGER.info("Authenticating successful with yotpo api")
        return self.__utoken


    def authenticate(self,headers :dict,params :dict,api_auth_version :str):
        if api_auth_version == "v1":
            params.update({"utoken":self._get_auth_token()})
        elif api_auth_version == "v3":
            headers.update({"X-Yotpo-Token":self._get_auth_token()})
        return headers,params
    
    @backoff.on_exception(wait_gen=lambda:1,exception=(errors.Http401RequestError,),jitter=None, max_tries=3)
    def get(self,endpoint,params,headers,api_auth_version) -> Any:
        headers,params = self.authenticate(headers,params,api_auth_version)
        return self.make_request("GET",endpoint,headers=headers,params=params)

    def post(self,endpoint,params,headers,api_auth_version,body)-> Any:
        headers,params = self.authenticate(headers,params,api_auth_version)
        self.make_request("POST",endpoint,headers=headers,params=params,data=body)

    @backoff.on_exception(wait_gen=lambda:60,exception=(errors.Http429RequestError,errors.Http500RequestError,errors.Http503RequestError,),jitter=None, max_tries=3)
    def make_request(self,method,endpoint,**kwargs) -> requests.Response or None:
        response = self._session.request(method,endpoint,**kwargs)
        if response.status_code != 200:
            try:
                raise_for_error(response)
            except errors.Http401RequestError as _:
                self._get_auth_token(regenerate_token=True)
                raise_for_error(response)
            return None
        return response.json()

