from .abstracts import IncremetalStream
from singer import metrics,write_record
import singer
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

    def get_url_endpoint(self) -> str:
        """
        Returns a formated endpoint using the stream attributes
        """
        return self.url_endpoint.replace("APP_KEY", self.client.config["api_key"])

    def get_records(self):
        params,headers = {"page":1,"per_page": 150,"sort": ["date", "time"],"direction": "asc"},{}
        base_url,shared_product_ids =  self.get_url_endpoint(),Products(self.client).prefetch_product_ids()
        total_product_count = len(shared_product_ids)
        for current,product_id in enumerate(shared_product_ids,1):
            call_next,params["page"] = True, 1
            extraction_url =  base_url.replace("PRODUCT_ID", product_id)
            while call_next:
                response =  self.client.get(extraction_url,params,headers,self.api_auth_version)
                raw_records = response.get("response",{}).get("reviews",[])
                if not raw_records:
                    call_next =  False
                params["page"]+=1
                yield from map(lambda _:(product_id,_),raw_records)
            LOGGER.info("Extracted reviews for product %s of %s",current,total_product_count)

    def sync(self,state,schema,stream_metadata,transformer):
        max_bookmark_value = singer.get_bookmark(state,self.tap_stream_id,self.replication_key,self.client.config[self.config_start_key])
        max_created_at = max_bookmark_value
        with metrics.record_counter(self.tap_stream_id) as counter:
            for product_id,record in self.get_records():
                record["domain_key"] = product_id
                transformed_record = transformer.transform(record, schema, stream_metadata)
                if record[self.replication_key] >= max_bookmark_value:
                    write_record(self.tap_stream_id, transformed_record)
                    max_created_at = max(record[self.replication_key],max_created_at)
                    counter.increment()
        state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_created_at)
        singer.write_state(state)
        return state

    def filter_record(self,record,state) -> bool:
        return True

