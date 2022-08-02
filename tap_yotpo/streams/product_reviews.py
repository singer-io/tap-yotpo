from .abstracts import IncremetalStream
from singer import metrics,write_record
import singer
import re

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

    def get_url_endpoint(self) -> str:
        """
        Returns a formated endpoint using the stream attributes
        """
        return self.url_endpoint.replace("APP_KEY", self.client.config["api_key"])

    def get_records(self):
        page_size = 100
        params,headers = {"page":1,"per_page": page_size,"sort": ["date", "time"],"direction": "asc"},{}
        base_url =  self.get_url_endpoint()
        for product_id in self.client.shared_product_ids:
            call_next,params["page"] = True, 1
            extraction_url =  base_url.replace("PRODUCT_ID", product_id)            
            while call_next:
                response =  self.client.get(extraction_url,params,headers,self.api_auth_version)
                LOGGER.info("response...............: %s",response)
                raw_records = response.get("response",{}).get(self.stream,[]) 
                if not raw_records:
                    call_next =  False
                params["page"]+=1
                yield from raw_records

    # def format_url(self):
    #     for product_id in self.client.shared_product_ids:
    #         if re.match("^[A-Za-z0-9_-]*$", product_id) is None:
    #             LOGGER.warning(f"Product-id - {product_id} contains special character. Processing next product-id")
    #             continue
    #         extraction_url =  self.get_url_endpoint(product_id)
    #         return extraction_url

    def sync(self,state,schema,stream_metadata,transformer,write_records=True):
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(record, schema, stream_metadata)
                if self.filter_record(record,state):
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
        return state


    def filter_record(self,record,state) ->bool:
        """
        Returns boolean if a record should be written
        """
        return True



