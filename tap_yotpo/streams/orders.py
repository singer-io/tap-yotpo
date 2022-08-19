"""tap-yotpo Orders stream module."""
from typing import Dict, Iterator
from singer.utils import strftime, strptime_to_utc


from singer import Transformer, get_logger, metrics, write_record

from ..helpers import ApiSpec
from .abstracts import IncremetalStream, UrlEndpointMixin

LOGGER = get_logger()


class Orders(IncremetalStream, UrlEndpointMixin):
    """class for Orders stream."""

    stream = "orders"
    tap_stream_id = "orders"
    key_properties = ["yotpo_id"]
    replication_key = "updated_at"
    valid_replication_keys = ["updated_at"]
    api_auth_version = ApiSpec.API_V3
    config_start_key = "start_date"
    url_endpoint = "https://api.yotpo.com/core/v3/stores/APP_KEY/orders"

    def get_records(self) -> Iterator[Dict]:
        """performs api querying and pagination of response."""
        extraction_url = self.get_url_endpoint()
        page_count,params= 1,{"limit": 10}
        while True:
            LOGGER.info("Calling Page %s",page_count)
            response = self.client.get(extraction_url, params, {}, self.api_auth_version)

            # retrive records from response.Orders key
            raw_records = response.get(self.stream, [])

            # retrive pagination from response.pagination.next_page_info key
            next_param = response.get("pagination", {}).get("next_page_info", None)

            if not raw_records or not next_param:
                LOGGER.warning("No records found on Page %s",page_count)
                break

            params["page_info"] = next_param
            page_count+=1
            yield from raw_records
    
    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Abstract implementation for `type: Incremental` stream."""
        LOGGER.info(stream_metadata)
        current_bookmark_date = self.get_bookmark(state)
        max_bookmark = current_bookmark_date_utc = strptime_to_utc(current_bookmark_date)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                try:
                    record_timestamp = strptime_to_utc(record[self.replication_key])
                except IndexError as _:
                    LOGGER.error("Unable to process Record, Exception occured: %s for stream %s",_,self.__class__)
                    continue
                if record_timestamp >= current_bookmark_date_utc:
                    LOGGER.info("writing record")
                    transformed_record = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    max_bookmark = max(max_bookmark, record_timestamp)
                else:
                    LOGGER.warning("Skipping Record Older than the timestamp")

            state = self.write_bookmark(state, value=strftime(max_bookmark))
        return state

