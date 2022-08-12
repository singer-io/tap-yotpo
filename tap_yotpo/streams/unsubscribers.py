from .abstracts import FullTableStream,UrlEndpointMixin
from singer import metrics,write_record,get_logger
LOGGER = get_logger()
from ..helpers import ApiSpec

class Unsubscribers(FullTableStream,UrlEndpointMixin):
    """
    class for products stream
    """
    stream = "unsubscribers"
    tap_stream_id = "unsubscribers"
    key_properties = ["id",]
    api_auth_version = ApiSpec.API_V1
    url_endpoint = "https://api.yotpo.com/apps/APP_KEY/unsubscribers"
    
    def get_records(self):
        extraction_url =  self.get_url_endpoint()
        params = {"page":1,"count":1000},
        while True:
            response =  self.client.get(extraction_url,params,{},self.api_auth_version)
            raw_records = response.get("response",{}).get(self.stream,[])
            if not raw_records:
                break
            params["page"]+=1
            yield from raw_records

    def sync(self,state,schema,stream_metadata,transformer):
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(record, schema, stream_metadata)
                write_record(self.tap_stream_id, transformed_record)
                counter.increment()
        return state 