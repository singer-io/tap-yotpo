"""tap-yotpo email stream module"""
import singer
from singer.utils import strptime_to_utc
from singer import metrics

from ..helpers import ApiSpec
from .abstracts import IncremetalStream, UrlEndpointMixin

LOGGER = singer.get_logger()

from datetime import datetime


class Emails(IncremetalStream, UrlEndpointMixin):
    """
    class for emails stream
    """

    stream = "emails"
    tap_stream_id = "emails"
    key_properties = ["email_address"]
    replication_key = "email_sent_timestamp"
    valid_replication_keys = ["email_sent_timestamp"]
    api_auth_version = ApiSpec.API_V1
    config_start_key = "start_date"
    url_endpoint = "https://api.yotpo.com/analytics/v1/emails/APP_KEY/export/raw_data"

    def get_records(self, bookmark_date):
        extraction_url =  self.get_url_endpoint()
        next_page, page_size = True, 5  #1000

        params,headers = {"page":1,
                         "per_page":page_size,
                         "sort":"descending",
                         "since":bookmark_date,
                         "until":datetime.today().strftime("%Y-%m-%d")},{}
        while next_page:
            response =  self.client.get(extraction_url,params,headers,self.api_auth_version)
            raw_records = response.get("records",[])
            if not raw_records:
                break
            
            params["page"]+=1

            yield from raw_records

    def sync(self,state,schema,stream_metadata,transformer):
        with metrics.record_counter(self.tap_stream_id) as counter:
            bookmark_date = singer.get_bookmark(state,self.tap_stream_id,self.replication_key, self.client.config["start_date"])
            current_max_bookmark_date = bookmark_date_to_utc = strptime_to_utc(bookmark_date)
            for record in self.get_records(bookmark_date):
                record_timestamp =  strptime_to_utc(record[self.replication_key])
                if record_timestamp >= bookmark_date_to_utc:
                    singer.write_record(self.tap_stream_id, transformer.transform(record, schema, stream_metadata))
                    current_max_bookmark_date = max(current_max_bookmark_date,record_timestamp)
                    counter.increment()
                else:
                    break

            state = singer.write_bookmark(state, self.tap_stream_id,
                                         self.replication_key, current_max_bookmark_date.strftime("%Y-%m-%d"))
        return state

