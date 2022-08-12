from singer import get_logger
from .abstracts import FullTableStream, UrlEndpointMixin
from ..helpers import ApiSpec
LOGGER = get_logger()


class Unsubscribers(FullTableStream, UrlEndpointMixin):
    """
    class for products stream
    """

    stream = "unsubscribers"
    tap_stream_id = "unsubscribers"
    key_properties = ["id",]
    api_auth_version = ApiSpec.API_V1
    url_endpoint = "https://api.yotpo.com/apps/APP_KEY/unsubscribers"

    def get_records(self):
        extraction_url = self.get_url_endpoint()
        params = ({"page": 1, "count": 1000},)
        while True:
            response = self.client.get(extraction_url, params, {}, self.api_auth_version)
            raw_records = response.get("response", {}).get(self.stream, [])
            if not raw_records:
                break
            params["page"] += 1
            yield from raw_records
