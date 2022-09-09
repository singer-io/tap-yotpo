"""tap-yotpo reviews stream module."""
from datetime import timedelta
from typing import Dict, Iterator, Optional

from singer import get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from ..helpers import ApiSpec
from .abstracts import IncrementalStream, UrlEndpointMixin

LOGGER = get_logger()


class Reviews(IncrementalStream, UrlEndpointMixin):
    """class for `reviews` stream."""

    stream = "reviews"
    tap_stream_id = "reviews"
    key_properties = ["id"]
    replication_key = "updated_at"
    valid_replication_keys = ["updated_at"]
    config_start_key = "start_date"
    api_auth_version = ApiSpec.API_V1
    url_endpoint = "https://api.yotpo.com/v1/apps/APP_KEY/reviews"

    def get_records(self, start_date: Optional[str]) -> Iterator[Dict]:
        """performs querying and pagination of reviews resource."""
        # pylint: disable=W0221
        extraction_url = self.get_url_endpoint()
        params = {"page": 1, "count": 100, "since_updated_at": start_date}
        while True:
            response = self.client.get(extraction_url, params, {}, self.api_auth_version)
            raw_records = response.get(self.stream, [])
            if not raw_records:
                break
            LOGGER.info("Fetched Reviews from page: %s", params["page"])
            params["page"] += 1
            yield from raw_records

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer) -> Dict:
        """Sync implementation for `reviews` stream."""
        max_bookmark = bookmark_date_utc = strptime_to_utc(self.get_bookmark(state))
        bookmark_date_utc = bookmark_date_utc - timedelta(days=int(self.client.config.get("reviews_lookback_days", 0)))

        with metrics.Counter(self.tap_stream_id) as counter:
            for record in self.get_records(strftime(bookmark_date_utc)):
                try:
                    record_timestamp = strptime_to_utc(record[self.replication_key])
                    if record_timestamp >= bookmark_date_utc:
                        max_bookmark = max(max_bookmark, record_timestamp)
                        transformed_record = transformer.transform(record, schema, stream_metadata)
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()
                    else:
                        LOGGER.info("Older Record Found")
                except TypeError:
                    LOGGER.info("unable to Find Replication Key for record")

            state = self.write_bookmark(state, value=strftime(max_bookmark))
        return state
