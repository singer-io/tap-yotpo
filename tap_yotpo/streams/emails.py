from .abstracts import IncremetalStream,FullTableStream
from singer import metrics,write_record
import singer
#from .products import Products
#import pendulum
from singer.utils import strptime_to_utc
LOGGER = singer.get_logger()

from datetime import datetime


class Emails(FullTableStream):
    """
    class for emails stream
    """
    stream = "emails"
    tap_stream_id = "emails"
    key_properties = ["email_address","email_sent_timestamp"]
    replication_key = "email_sent_timestamp"
    valid_replication_keys = ["email_sent_timestamp"]
    api_auth_version = "v1"
    config_start_key = "start_date"
    url_endpoint = "https://api.yotpo.com/analytics/v1/emails/APP_KEY/export/raw_data"

    def get_url_endpoint(self) -> str:
        """
        Returns a formated endpoint using the stream attributes
        """
        return self.url_endpoint.replace("APP_KEY", self.client.config["api_key"])

    def get_records(self, bookmark_date):
        extraction_url =  self.get_url_endpoint()
        next_page, page_size = True, 5  #1000
        #lookback_days = self.client.config["email_stats_lookback_days"]
        # since_date = pendulum.parse(self.client.config["start_date"]) \
        #                      .in_timezone("UTC") \
        #                      .add(days=-lookback_days)
        #until_date = pendulum.tomorrow().in_timezone("UTC")
        bookmark_date = strptime_to_utc(bookmark_date)
        until_date = strptime_to_utc(datetime.today())

        params,headers = {"page":1,
                         "per_page":page_size,
                         "sort":"descending",
                         "since":bookmark_date.to_date_string(),
                         "until":until_date.to_date_string()},{}
        while next_page:
            response =  self.client.get(extraction_url,params,headers,self.api_auth_version)
            raw_records = response.get("records",[])
            if not raw_records:
                break
            for record in raw_records:
                record_timestamp =  strptime_to_utc(record[self.replication_key])
                if record_timestamp >= bookmark_date:
                    current_max =  max(current_max,record_timestamp)
                else:
                    next_page = False

            params["page"]+=1
        return (raw_records, current_max)


    def sync(self,state,schema,stream_metadata,transformer):
        with metrics.record_counter(self.tap_stream_id) as counter:
            bookmark_date = singer.get_bookmark(state,self.tap_stream_id,"since_date",self.client.config["start_date"])
            raw_records, new_bookmark_date = self.get_records(bookmark_date)
            for record in self.get_records():
                transformed_record = transformer.transform(record, schema, stream_metadata)
                write_record(self.tap_stream_id, transformed_record)
                counter.increment()
        return state 