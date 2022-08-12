from typing import Dict, List

from singer import Transformer, get_logger, metrics, write_record
from ..helpers import ApiSpec
from .abstracts import FullTableStream,UrlEndpointMixin

LOGGER = get_logger()

class Products(FullTableStream,UrlEndpointMixin):
    """
    class for products stream
    """
    stream = "products"
    tap_stream_id = "products"
    key_properties = ["yotpo_id",]
    api_auth_version = ApiSpec.API_V3
    url_endpoint = "https://api.yotpo.com/core/v3/stores/APP_KEY/products"

    def get_records(self) -> List:
        extraction_url =  self.get_url_endpoint()
        headers,params,call_next = {},{"limit":100},True
        while call_next:
            response =  self.client.get(extraction_url,params,headers,self.api_auth_version)

            # retrive records from response.products key
            raw_records = response.get(self.stream,[])

            # retrive pagination from response.pagination.next_page_info key
            next_param = response.get("pagination",{}).get("next_page_info",None)

            if not raw_records or not next_param:
                call_next =  False

            params["page_info"]= next_param
            yield from raw_records

    def sync(self,state :Dict,schema :Dict,stream_metadata :Dict,transformer :Transformer) ->Dict:
        shared_product_ids = []
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():

                transformed_record = transformer.transform(record, schema, stream_metadata)
                write_record(self.tap_stream_id, transformed_record)
                counter.increment()

                try:
                    # creating a cache of product_ids for `product_reviews` stream
                    shared_product_ids.append((record["yotpo_id"],record["external_id"]))
                except KeyError as _:
                    LOGGER.warning("Unable to find external product ID")

        self.client.shared_product_ids = sorted(shared_product_ids,key=lambda _:_[0])
        return state

    def prefetch_product_ids(self,):
        """
        Helper method implemented for other streams to load all product_ids which are required to fetch `product_reviews`
        """
        prod_ids =  getattr(self.client,"shared_product_ids",[])
        if not prod_ids:
            LOGGER.info("Fetching all product records")
            for record in self.get_records():
                try:
                    prod_ids.append((record["yotpo_id"],record["external_id"]))
                except KeyError as _:
                    LOGGER.warning("Unable to find external product ID")

            self.client.shared_product_ids = sorted(prod_ids,key=lambda _:_[0])
        return prod_ids
