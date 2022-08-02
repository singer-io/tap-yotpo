from .abstracts import IncremetalStream


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
    url_endpoint = " https://api-cdn.yotpo.com/v1/widget/:APP_KEY/products/PRODUCT_ID/reviews.json"

    def get_url_endpoint(self) -> str:
        """
        Returns a formated endpoint using the stream attributes
        """
        for product_id in self.client.shared_product_ids:
            return self.url_endpoint.replace("APP_KEY", self.client.config["api_key"]).replace("PRODUCT_ID",product_id)

    def get_records(self):
        extraction_url =  self.get_url_endpoint()
        call_next, page_size = True, 100
        params,headers = {"page":1,"per_page": page_size,"sort": ["date", "time"],"direction": "asc"},{}

        while call_next:
            response =  self.client.get(extraction_url,params,headers,self.api_auth_version)
            raw_records = response.get(self.stream,[])
            if not raw_records:
                call_next =  False
            params["page"]+=1
            yield from raw_records

