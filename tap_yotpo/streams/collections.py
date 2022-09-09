"""tap-yotpo collections stream module."""
from typing import Dict, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from ..helpers import ApiSpec
from .abstracts import IncrementalStream, UrlEndpointMixin

LOGGER = get_logger()
DATE_FORMAT = "%Y-%m-%d"


class Collections(IncrementalStream, UrlEndpointMixin):
    """class for collections stream."""

    stream = "collections"
    tap_stream_id = "collections"
    key_properties = ["yotpo_id"]
    replication_key = "updated_at"
    valid_replication_keys = ["updated_at"]
    api_auth_version = ApiSpec.API_V3
    config_start_key = "start_date"
    url_endpoint = "https://api.yotpo.com/core/v3/stores/APP_KEY/collections"

    def get_records(self) -> List:
        """performs api querying and pagination of response."""
        extraction_url = self.get_url_endpoint()
        page_count, params = 1, {}
        while True:
            LOGGER.info("Calling Page %s", page_count)
            response = self.client.get(extraction_url, params, {}, self.api_auth_version)

            # retrive records from response.collections key
            raw_records = response.get(self.stream, [])

            # retrieve pagination from response.pagination.next_page_info key
            next_param = response.get("pagination", {}).get("next_page_info", None)

            if not raw_records:
                LOGGER.warning("No records found on Page %s", page_count)
                break
            params["page_info"] = next_param
            page_count += 1
            yield from raw_records
            if not next_param:
                break

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Sync implementation for `collections` stream."""
        bookmark_date = self.get_bookmark(state)
        current_max_bookmark_date = bookmark_date_to_utc = strptime_to_utc(bookmark_date)
        skip_record_count = 0
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                try:
                    record_timestamp = strptime_to_utc(record[self.replication_key])
                except KeyError as _:
                    LOGGER.error("Unable to process Record, Exception occured: %s for stream %s", _, self.__class__)
                    continue
                if record_timestamp >= bookmark_date_to_utc:
                    write_record(self.tap_stream_id, transformer.transform(record, schema, stream_metadata))
                    current_max_bookmark_date = max(current_max_bookmark_date, record_timestamp)
                    counter.increment()
                else:
                    skip_record_count += 1

            state = self.write_bookmark(state, value=strftime(current_max_bookmark_date))
        LOGGER.warning(f"Total number of records skipped due to older timestamp = {skip_record_count}")
        return state
