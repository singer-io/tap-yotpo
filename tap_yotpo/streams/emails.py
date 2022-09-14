"""tap-yotpo email stream module."""
from datetime import datetime, timedelta
from typing import Dict, Iterator

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from ..helpers import ApiSpec
from .abstracts import IncrementalStream, UrlEndpointMixin

LOGGER = get_logger()
DATE_FORMAT = "%Y-%m-%d"


class Emails(IncrementalStream, UrlEndpointMixin):
    """class for emails stream."""

    stream = "emails"
    tap_stream_id = "emails"
    key_properties = ["email_address", "email_sent_timestamp"]
    replication_key = "email_sent_timestamp"
    valid_replication_keys = ["email_sent_timestamp"]
    api_auth_version = ApiSpec.API_V1
    config_start_key = "start_date"
    url_endpoint = "https://api.yotpo.com/analytics/v1/emails/APP_KEY/export/raw_data"

    def get_records(self, start_date: str) -> Iterator[Dict]:
        """performs querying and pagination of email resource."""
        # pylint: disable=W0221
        extraction_url = self.get_url_endpoint()
        params = {
            "page": 1,
            "per_page": 1000,
            "sort": "descending",
            "since": start_date,
            "until": datetime.today().strftime(DATE_FORMAT),
        }
        while True:
            response = self.client.get(extraction_url, params, {}, self.api_auth_version)
            raw_records = response.get("records", [])
            if not raw_records:
                break
            params["page"] += 1
            yield from raw_records

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Sync implementation for `emails` stream."""

        lookback_window = self.client.config.get("email_stats_lookback_days", 0)
        max_bookmark = bookmark_date_utc = strptime_to_utc(self.get_bookmark(state))
        bookmark_date_utc = bookmark_date_utc - timedelta(days=int(lookback_window))
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(bookmark_date_utc.strftime(DATE_FORMAT)):
                record_timestamp = strptime_to_utc(record[self.replication_key])
                if record_timestamp >= bookmark_date_utc:
                    write_record(self.tap_stream_id, transformer.transform(record, schema, stream_metadata))
                    max_bookmark = max(max_bookmark, record_timestamp)
                    counter.increment()
                else:
                    break
            state = self.write_bookmark(state, value=strftime(max_bookmark))
        return state
