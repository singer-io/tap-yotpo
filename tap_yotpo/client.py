from typing import Any
from requests import session
import singer


LOGGER = singer.get_logger()

class Client:
    def __init__(self,config) -> None:
        self.config = config
        self.session = session()
        self.__utoken = None

    def _get_auth_token(self,regenerate_token=False):
        if self.__utoken and not regenerate_token:
            return self.__utoken
        self.__utoken = self.config["utoken"]
        return self.__utoken


    def authenticate(self,headers :dict,params :dict,api_auth_version :str):
        if api_auth_version == "v1":
            params.update({"utoken":self._get_auth_token()})
        elif api_auth_version == "v3":
            headers.update({"X-Yotpo-Token":self._get_auth_token()})
        return headers,params
    
    def get(self,endpoint,params,headers,api_auth_version) -> Any:
        headers,params = self.authenticate(headers,params,api_auth_version)
        return self.make_request("GET",endpoint,headers=headers,params=params)

    def post(self,endpoint,params,headers,api_auth_version,body)-> Any:
        headers,params = self.authenticate(headers,params,api_auth_version)
        self.make_request("POST",endpoint,headers=headers,params=params,body=body)


    def make_request(self,method,endpoint,**kwargs):
        response = self.session.request(method,endpoint,**kwargs)
        if response.status_code != 200:
            LOGGER.info("api failed status %s",response.text)
            return None
        # LOGGER.info("api suceeded url %s",response.url)
        return response.json()
