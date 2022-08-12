"""tap-yotpo reviews stream module"""
from typing import Dict, List, Optional

from singer import get_bookmark, get_logger, metrics, write_bookmark, write_record
from singer.utils import strptime_to_utc

from ..helpers import ApiSpec
from .abstracts import IncremetalStream, UrlEndpointMixin

LOGGER = get_logger()


class Reviews(IncremetalStream, UrlEndpointMixin):
    """
    class for products stream
    """

    stream = "reviews"
    tap_stream_id = "reviews"
    key_properties = [
        "id",
    ]
    replication_key = "updated_at"
    valid_replication_keys = [
        "updated_at",
    ]
    config_start_key = "start_date"
    api_auth_version = ApiSpec.API_V1
    url_endpoint = "https://api.yotpo.com/v1/apps/APP_KEY/reviews"

    def get_records(self, start_date: Optional[str]) -> List:
        # pylint: disable=W0221
        extraction_url = self.get_url_endpoint()
        params = {"page": 1, "count": 150, "since_updated_at": start_date}
        call_next = True
        while call_next:
            LOGGER.info("Fetching Reviews Page: %s", params["page"])
            response = self.client.get(extraction_url, params, {}, self.api_auth_version)
            raw_records = response.get(self.stream, [])
            if not raw_records:
                break
            params["page"] += 1
            yield from raw_records

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer) -> Dict:
        """
        Sync implementation for `reviews` stream
        """
        config_start = self.client.config[self.config_start_key]
        bookmark_date = get_bookmark(state, self.tap_stream_id, self.replication_key, config_start)
        max_updated_at = strptime_to_utc(bookmark_date)

        with metrics.Counter(self.tap_stream_id) as counter:
            for record in self.get_records(bookmark_date):
                try:
                    max_updated_at = max(max_updated_at, strptime_to_utc(record[self.replication_key]))
                except TypeError:
                    LOGGER.info("unable to Find Replication Key for record")

                transformed_record = transformer.transform(record, schema, stream_metadata)
                write_record(self.tap_stream_id, transformed_record)
                counter.increment()

            state = write_bookmark(
                state, self.tap_stream_id, self.replication_key, max_updated_at.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
        return state
