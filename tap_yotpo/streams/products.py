from typing import Dict

from .abstracts import IncremetalStream


class Products(IncremetalStream):
    """
    class for products stream
    """
    stream = "products"
    tap_stream_id = "products"
    key_properties = ["id",]
    replication_key = "updated_at"
    valid_replication_keys = ["created_at","updated_at"]
    api_auth_version = "v1"
    config_start_key = "start_date"
    url_endpoint = "https://api.yotpo.com/v1/apps/APP_KEY/products"

    def get_url_endpoint(self) -> str:
        """
        Returns a formated endpoint using the stream attributes
        """
        return self.url_endpoint.replace("APP_KEY", self.client.config["api_key"])
    
    def get_records(self):
        extraction_url =  self.get_url_endpoint()
        call_next, page_size = True, 1000
        params,headers = {"page":1,"count":page_size},{}
        while call_next:
            response =  self.client.get(extraction_url,params,headers,self.api_auth_version)
            raw_records = response.get(self.stream,[])
            if not raw_records:
                call_next =  False
            params["page"]+=1
            yield from raw_records

    def filter_record(self,record :Dict,state :Dict) ->bool:
        """
        Returns boolean if a record should be written
        """
        prev_bookmark_val = self.get_bookmark(state)
        record_bookmark_value = record[self.replication_key]
        return True if record_bookmark_value > prev_bookmark_val else False