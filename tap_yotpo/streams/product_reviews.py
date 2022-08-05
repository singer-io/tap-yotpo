import re
from asyncio.log import logger

import singer
from singer import metrics, write_record

from .abstracts import IncremetalStream
from .products import Products

LOGGER = singer.get_logger()


class ProductReviews(IncremetalStream):
    """
    class for product_reviews stream
    """
    stream = "product_reviews"
    tap_stream_id = "product_reviews"
    key_properties = ["id",]
    replication_key = "created_at"
    valid_replication_keys = ["created_at"]
    api_auth_version = "v1"
    config_start_key = "start_date"
    url_endpoint = " https://api-cdn.yotpo.com/v1/widget/APP_KEY/products/PRODUCT_ID/reviews.json"

    def __init__(self, client=None) -> None:
        self.sync_prod :bool = True
        self.last_synced :bool  =  False
        super().__init__(client)

    def get_url_endpoint(self) -> str:
        """
        Returns a formated endpoint using the stream attributes
        """
        return self.url_endpoint.replace("APP_KEY", self.client.config["api_key"])

    def get_records(self):
        params,headers = {"page":1,"per_page": 150,"sort": ["date", "time"],"direction": "desc"},{}
        base_url,shared_product_ids =  self.get_url_endpoint(),Products(self.client).prefetch_product_ids()
        total_product_count,last_sync_index = len(shared_product_ids),0
        if self.last_synced:
            for pos,(prod_id,_) in enumerate(shared_product_ids):
                if prod_id == self.last_synced:
                    last_sync_index,self.last_synced = pos+1,False
                    break
        for current,(y_prod_id,ext_prod_id) in enumerate(shared_product_ids[last_sync_index:],last_sync_index):
            if self.skip_product(ext_prod_id):
                LOGGER.warning("Product skipped due to malformed external-product id")
                continue
            params["page"],self.sync_prod = 1,True
            extraction_url =  base_url.replace("PRODUCT_ID", ext_prod_id)

            while True:
                if not self.sync_prod:
                    break
                LOGGER.info("fetching page %s of product %s",params["page"],ext_prod_id)
                response =  self.client.get(extraction_url,params,headers,self.api_auth_version)
                if not response:
                    break
                raw_records = response.get("response",{}).get("reviews",[])
                if not raw_records:
                    break
                try:
                    domain_key = response.get("response",{}).get("products",[])[0].get("domain_key")
                except IndexError as _:
                    LOGGER.warning("%s domain key not found",current)
                params["page"]+=1
                yield from map(lambda _:(y_prod_id,domain_key,_),raw_records)
            LOGGER.info("Extraction complete for `product_reviews` Product %s of %s",current,total_product_count,)


    def sync(self,state,schema,stream_metadata,transformer):
        max_bookmark_value = singer.get_bookmark(state,self.tap_stream_id,self.replication_key,self.client.config[self.config_start_key])
        self.last_synced = singer.get_bookmark(state,self.tap_stream_id,"last_synced",False)
        max_created_at = singer.get_bookmark(state,self.tap_stream_id,"last_synced_max_datetime",max_bookmark_value)

        prev_prod_id = None
        for prod_id,domain_key,record in self.get_records():
            record["domain_key"] = domain_key
            transformed_record = transformer.transform(record, schema, stream_metadata)
            if record[self.replication_key] >= max_bookmark_value:
                write_record(self.tap_stream_id, transformed_record)
                max_created_at = max(record[self.replication_key],max_created_at)
            else:
                self.sync_prod = False

            if prev_prod_id is None:
                prev_prod_id = prod_id
            elif prev_prod_id != prod_id:
                state = singer.write_bookmark(state, self.tap_stream_id, "last_synced", prev_prod_id)
                state = singer.write_bookmark(state, self.tap_stream_id, "last_synced_max_datetime", max_created_at)
                singer.write_state(state)
                prev_prod_id = prod_id
        state = singer.clear_bookmark(state, self.tap_stream_id, "last_synced")
        state = singer.clear_bookmark(state, self.tap_stream_id, "last_synced_max_datetime")
        state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_created_at)
        singer.write_state(state)
        return state

    def skip_product(self,prod_id) -> bool:
        return True if re.match("^[A-Za-z0-9_-]*$", prod_id) is None else False
