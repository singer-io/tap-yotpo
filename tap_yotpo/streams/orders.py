"""tap-yotpo Orders stream module."""
from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from ..helpers import ApiSpec
from .abstracts import IncrementalStream, UrlEndpointMixin

LOGGER = get_logger()


class Orders(IncrementalStream, UrlEndpointMixin):
    """class for Orders stream."""

    stream = "orders"
    tap_stream_id = "orders"
    key_properties = ["yotpo_id"]
    replication_key = "order_date"
    valid_replication_keys = ["order_date"]
    api_auth_version = ApiSpec.API_V3
    config_start_key = "start_date"
    url_endpoint = "https://api.yotpo.com/core/v3/stores/APP_KEY/orders"

    def get_records(self, start_date: str = None) -> Iterator[Dict]:
        """performs api querying and pagination of response."""
        extraction_url = self.get_url_endpoint()
        page_count, params = 1, {}
        if start_date:
            params["order_date_min"] = strftime(start_date)
        else:
            LOGGER.info("Executing Order Stream without date filter %s", params)

        while True:
            LOGGER.info("Fetching Page %s", page_count)
            response = self.client.get(extraction_url, params, {}, self.api_auth_version)

            # retrieve records from response.orders key
            raw_records = response.get(self.stream, [])

            # retrieve pagination from response.pagination.next_page_info key
            next_param = response.get("pagination", {}).get("next_page_info", None)

            if not raw_records:
                LOGGER.warning("No records found on Page %s", page_count)
                break
            params["page_info"] = next_param

            # if `order_date_min` param is passed with page_info, it will break the api resulting in 400 error
            # the date is stored in the page_info cursor which sends the filtered records without requiring
            # the filter param for further pages
            if "order_date_min" in params:
                del params["order_date_min"]
            page_count += 1
            yield from raw_records
            if not next_param:
                break

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Sync implementation for `orders` stream."""
        current_bookmark_date = self.get_bookmark(state)
        max_bookmark = current_bookmark_date_utc = strptime_to_utc(current_bookmark_date)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(current_bookmark_date_utc):
                try:
                    record_timestamp = strptime_to_utc(record[self.replication_key])
                except IndexError as _:
                    LOGGER.error("Unable to process Record, Exception occurred: %s for stream %s", _, self.__class__)
                    continue
                if record_timestamp >= current_bookmark_date_utc:
                    write_record(self.tap_stream_id, transformer.transform(record, schema, stream_metadata))
                    max_bookmark = max(max_bookmark, record_timestamp)
                    counter.increment()
                else:
                    LOGGER.warning("Skipping Older Record, order-id - ******%s", str(record["yotpo_id"])[-4:])

            state = self.write_bookmark(state, value=strftime(max_bookmark))
        return state

    def prefetch_order_ids(self) -> List:
        """Helper method implemented for other streams to load all order_ids.

        eg: orders are required to fetch `fullfilment` stream
        """
        order_ids = getattr(self.client, "shared_order_ids", [])
        if not order_ids:
            LOGGER.info("Fetching all Order_ids")
            for record in self.get_records():
                try:
                    order_ids.append((record["yotpo_id"], record["external_id"]))
                except KeyError:
                    LOGGER.warning("Unable to find external order ID or Yotpo ID")

            self.client.shared_order_ids = sorted(order_ids, key=lambda _: _[0])
        return order_ids
