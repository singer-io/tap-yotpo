
from .abstracts import FullTableStream
from singer import metrics,write_record,get_logger
LOGGER = get_logger()

class Products(FullTableStream):
    """
    class for products stream
    """
    stream = "products"
    tap_stream_id = "products"
    key_properties = ["yotpo_id",]
    api_auth_version = "v3"
    url_endpoint = "https://api.yotpo.com/core/v3/stores/STORE_ID/products"

    def get_url_endpoint(self) -> str:
        """
        Returns a formated endpoint using the stream attributes
        """
        return self.url_endpoint.replace("STORE_ID", self.client.config["api_key"])
    
    def get_records(self):
        extraction_url =  self.get_url_endpoint()
        call_next, page_size = True,100
        params,headers = {"limit":page_size},{}
        while call_next:
            response =  self.client.get(extraction_url,params,headers,self.api_auth_version)
            #LOGGER.info("responseeeeeee....... %s",response)
            raw_records = response.get(self.stream,[])
            LOGGER.info("raw_records....... %s",response)
            pagination = response.get("pagination",[])
            if not raw_records:
                call_next =  False
                #pagination["next_page_info"] = None
            #params["page"]+=1
            params[""]= pagination["next_page_info"]

            yield from raw_records

    def sync(self,state,schema,stream_metadata,transformer):
        shared_product_ids = []
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(record, schema, stream_metadata)
                write_record(self.tap_stream_id, transformed_record)
                counter.increment()
                try:
                    shared_product_ids.append(record["external_id"])
                except KeyError as _:
                    LOGGER.warning("Unable to find external product ID")
        self.client.shared_product_ids = shared_product_ids
        return state

    def prefetch_product_ids(self,):
        """
        Helper method implemented for `product_reviews` to load all product_ids which are required to fetch `product_reviews`
        """
        prod_ids =  getattr(self.client,"shared_product_ids",[])
        if not prod_ids:
            for record in self.get_records():
                try:
                    prod_ids.append(record["external_product_id"])
                except KeyError as _:
                    LOGGER.warning("Unable to find external product ID")
            self.client.shared_product_ids = prod_ids
        return prod_ids
                