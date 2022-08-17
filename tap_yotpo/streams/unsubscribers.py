"""tap-yotpo unsubsrcibers stream module."""
from typing import Dict, Iterator

from singer import get_logger

from ..helpers import ApiSpec
from .abstracts import FullTableStream, UrlEndpointMixin

LOGGER = get_logger()


class Unsubscribers(FullTableStream, UrlEndpointMixin):
    """class for unsubscribers stream."""

    stream = "unsubscribers"
    tap_stream_id = "unsubscribers"
    key_properties = ["id"]
    api_auth_version = ApiSpec.API_V1
    url_endpoint = "https://api.yotpo.com/apps/APP_KEY/unsubscribers"

    def get_records(self) -> Iterator[Dict]:
        extraction_url = self.get_url_endpoint()
        params = {"page": 1, "count": 1000}
        while True:
            response = self.client.get(extraction_url, params, {}, self.api_auth_version)
            raw_records = response.get("response", {}).get(self.stream, [])
            if not raw_records:
                break
            params["page"] += 1
            yield from raw_records
