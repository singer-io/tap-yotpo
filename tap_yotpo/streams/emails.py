"""tap-yotpo email stream module"""
import pendulum
import singer

from .abstracts import FullTableStream, UrlEndpointMixin

LOGGER = singer.get_logger()


class Emails(FullTableStream, UrlEndpointMixin):
    """
    class for emails stream
    """

    stream = "emails"
    tap_stream_id = "emails"
    key_properties = ["email_address", "email_sent_timestamp"]
    replication_key = "email_sent_timestamp"
    valid_replication_keys = ["email_sent_timestamp"]
    api_auth_version = "v1"
    config_start_key = "start_date"
    url_endpoint = "https://api.yotpo.com/analytics/v1/emails/APP_KEY/export/raw_data"

    def get_records(self):
        extraction_url = self.get_url_endpoint()
        call_next, page_size = True, 1000
        lookback_days = self.client.config["email_stats_lookback_days"]
        since_date = pendulum.parse(self.client.config["start_date"]).in_timezone("UTC").add(days=-lookback_days)
        until_date = pendulum.tomorrow().in_timezone("UTC")

        params = {
            "page": 1,
            "per_page": page_size,
            "sort": "descending",
            "since": since_date.to_date_string(),
            "until": until_date.to_date_string(),
        }
        headers = {}
        while call_next:
            response = self.client.get(extraction_url, params, headers, self.api_auth_version)
            raw_records = response.get("records", [])
            if not raw_records:
                call_next = False
            params["page"] += 1
            yield from raw_records
