
from .abstracts import FullTableStream
from singer import metrics,write_record,get_logger
LOGGER = get_logger()

class Products(FullTableStream):
    """
    class for products stream
    """
    stream = "products"
    tap_stream_id = "products"
    key_properties = ["id",]
    #replication_key = "updated_at"
    #valid_replication_keys = ["created_at","updated_at"]
    api_auth_version = "v1"
    #config_start_key = "start_date"
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

    def sync(self,state,schema,stream_metadata,transformer,write_records=True):
        shared_product_ids = []
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(record, schema, stream_metadata)
                if write_records:
                    write_record(self.tap_stream_id, transformed_record)
                try:
                    shared_product_ids.append(record["external_product_id"])
                except KeyError as _:
                    LOGGER.warning("Unable to find external product ID")
                counter.increment()
        # the product_id's are required for the stream `product_reviews` as it fetches the reviews by product using external_product_id
        self.client.shared_product_ids = shared_product_ids
        return state 