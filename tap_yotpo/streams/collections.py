"""tap-yotpo collections stream module"""
from typing import Dict, List
import singer
from singer.utils import strptime_to_utc
from singer import metrics

from ..helpers import ApiSpec
from .abstracts import IncremetalStream, UrlEndpointMixin

LOGGER = singer.get_logger()

from datetime import datetime


class Collections(IncremetalStream, UrlEndpointMixin):
    """
    class for collections stream
    """

    stream = "collections"
    tap_stream_id = "collections"
    key_properties = ["yotpo_id"]
    replication_key = "updated_at"
    valid_replication_keys = ["updated_at"]
    api_auth_version = ApiSpec.API_V3
    config_start_key = "start_date"
    url_endpoint = "https://api.yotpo.com/core/v3/stores/APP_KEY/collections"

    def get_records(self) -> List:
        """
        performs api querying and pagination of response
        """
        extraction_url = self.get_url_endpoint()
        headers, params, call_next = {}, {"limit": 100}, True
        while call_next:
            response = self.client.get(extraction_url, params, headers, self.api_auth_version)

            raw_records = response.get(self.stream, [])

            # retrieve pagination from response.pagination.next_page_info key
            next_param = response.get("pagination", {}).get("next_page_info", None)

            if not raw_records or not next_param:
                call_next = False

            params["page_info"] = next_param
            yield from raw_records

    def sync(self, state, schema, stream_metadata, transformer):
        with metrics.record_counter(self.tap_stream_id) as counter:
            bookmark_date = singer.get_bookmark(state, self.tap_stream_id, self.replication_key,
                                                self.client.config["start_date"])
            current_max_bookmark_date = bookmark_date_to_utc = strptime_to_utc(bookmark_date)
            for record in self.get_records():
                record_timestamp = strptime_to_utc(record[self.replication_key])
                if record_timestamp >= bookmark_date_to_utc:
                    singer.write_record(self.tap_stream_id, transformer.transform(record, schema, stream_metadata))
                    current_max_bookmark_date = max(current_max_bookmark_date, record_timestamp)
                    counter.increment()
                else:
                    break

            state = singer.write_bookmark(state, self.tap_stream_id,
                                          self.replication_key, current_max_bookmark_date.strftime("%Y-%m-%d"))
        return state

