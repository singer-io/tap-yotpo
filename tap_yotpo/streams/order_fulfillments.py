"""tap-yotpo order-fulfillments stream module"""
from datetime import datetime
from typing import Dict, List, Tuple

import singer
from singer import (
    Transformer,
    clear_bookmark,
    get_bookmark,
    metrics,
    write_record,
    write_state,
)
from singer.utils import strftime, strptime_to_utc

from tap_yotpo.helpers import ApiSpec

from .abstracts import IncremetalStream, UrlEndpointMixin
from .orders import Orders

LOGGER = singer.get_logger()


class OrderFulfillments(IncremetalStream, UrlEndpointMixin):
    """
    class for Order fulfillments stream
    """

    stream = "order_fulfillments"
    tap_stream_id = "order_fulfillments"
    key_properties = ["id"]
    replication_key = "updated_at"
    valid_replication_keys = ["updated_at"]
    api_auth_version = ApiSpec.API_V3
    # points to the attribute of the config that marks the first-start-date for the stream
    config_start_key = "start_date"
    url_endpoint = "https://api.yotpo.com/core/v3/stores/APP_KEY/orders/ORDER_ID/fulfillments"

    def __init__(self, client=None) -> None:
        super().__init__(client)
        self.sync_prod: bool = True
        self.last_synced: bool = False
        self.base_url = self.get_url_endpoint()

    def get_orders(self, state) -> Tuple[List, int]:
        """
        Returns index for sync resuming on interruption
        """
        shared_order_ids = Orders(self.client).prefetch_order_ids()
        last_synced = singer.get_bookmark(state, self.tap_stream_id, "currently_syncing", False)
        last_sync_index = 0
        if last_synced:
            for pos, (order_id, _) in enumerate(shared_order_ids):
                if order_id == last_synced:
                    LOGGER.warning("Last Sync was interrupted after order *****%s", str(order_id)[-4:])
                    last_sync_index = pos
                    break
        return shared_order_ids, last_sync_index

    def get_records(self, order_id: str, bookmark_date: str) -> Tuple[List, datetime]:
        # pylint: disable=W0221
        """
        performs api querying and pagination of response
        """
        extraction_url = self.base_url.replace("ORDER_ID", order_id)
        bookmark_date = current_max = strptime_to_utc(bookmark_date)
        filtered_records = []
        page_count, params = 1, {}
        while True:
            LOGGER.info("Calling Page %s", page_count)

            response = self.client.get(extraction_url, params, {}, self.api_auth_version)

            raw_records = response.get("fulfillments", [])
            pagination = response.get("pagination", {}).get("next_page_info", None)

            if not raw_records:
                LOGGER.warning("No records found on Page %s", page_count)
                break

            for record in raw_records:
                record_timestamp = strptime_to_utc(record[self.replication_key])
                if record_timestamp >= bookmark_date:
                    current_max = max(current_max, record_timestamp)
                    filtered_records.append(record)

            if not pagination:
                break
            else:
                params['page_info'] = pagination
            page_count+=1

        return (filtered_records, current_max)

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """
        Sync implementation for `order_fulfillments` stream
        """
        # pylint: disable=R0914
        with metrics.Timer(self.tap_stream_id, None):
            config_start = self.client.config[self.config_start_key]
            orders, start_index = self.get_orders(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            order_len = len(orders)

            with metrics.Counter(self.tap_stream_id) as counter:
                for index, (order_id, ext_order_id) in enumerate(orders[start_index:], max(start_index, 1)):

                    LOGGER.info("Sync for order *****%s (%s/%s)", str(order_id)[-4:], index, order_len)

                    # If bookmark value not present in state, refer to the start-date from config
                    bookmark_date = get_bookmark(state, self.tap_stream_id, str(order_id), config_start)
                    records, max_bookmark = self.get_records(str(order_id), bookmark_date)

                    for _ in records:
                        write_record(self.tap_stream_id, transformer.transform(_, schema, stream_metadata))
                        counter.increment()

                    # bookmark value won't be updated for those order_id which are not having any latest fulfillments records.
                    if records:
                        state = self.write_bookmark(state, order_id, strftime(max_bookmark))
                    state = self.write_bookmark(state, "currently_syncing", order_id)
                    write_state(state)
            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")
        return state
