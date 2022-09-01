"""tap-yotpo customers stream module."""
from typing import Dict, Iterator

from singer import get_logger

from ..helpers import ApiSpec
from .abstracts import FullTableStream, UrlEndpointMixin

LOGGER = get_logger()


class Customers(FullTableStream, UrlEndpointMixin):
    """class for Customers stream."""

    stream = "customers"
    tap_stream_id = "customers"
    key_properties = []
    api_auth_version = ApiSpec.API_V3
    url_endpoint = "https://api.yotpo.com/core/v3/stores/APP_KEY/customers"

    def get_records(self) -> Iterator[Dict]:
        extraction_url = self.get_url_endpoint()
        page_count, params = 1, {}
        while True:
            query_string = ""
            if params.items():
                query_string = "&".join([f"{key}={value}" for (key, value) in params.items()])
            url = extraction_url + "?" + query_string
            LOGGER.info("Calling Page %s", page_count)
            response = self.client.get(url, {}, {}, self.api_auth_version)
            raw_records = response.get(self.stream, [])
            pagination = response.get("pagination", {}).get("next_page_info", None)

            yield from raw_records

            if not pagination:
                break
            else:
                params["page_info"] = pagination
            page_count += 1
