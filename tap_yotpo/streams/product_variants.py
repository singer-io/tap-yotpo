"""tap-yotpo product-variants stream module."""
from datetime import datetime
from typing import Dict, List, Tuple

from singer import (
    Transformer,
    clear_bookmark,
    get_bookmark,
    get_logger,
    metrics,
    write_record,
    write_state,
)
from singer.utils import strftime, strptime_to_utc

from tap_yotpo.helpers import ApiSpec

from .abstracts import IncrementalStream, UrlEndpointMixin
from .products import Products

LOGGER = get_logger()


class ProductVariants(IncrementalStream, UrlEndpointMixin):
    """class for product_variants stream."""

    stream = "product_variants"
    tap_stream_id = "product_variants"
    key_properties = ["yotpo_id"]
    replication_key = "updated_at"
    valid_replication_keys = ["updated_at"]
    api_auth_version = ApiSpec.API_V3
    # points to the attribute of the config that marks the first-start-date for the stream
    config_start_key = "start_date"
    url_endpoint = "https://api.yotpo.com/core/v3/stores/APP_KEY/products/PRODUCT_ID/variants"

    def __init__(self, client=None) -> None:
        super().__init__(client)
        self.sync_prod: bool = True
        self.last_synced: bool = False
        self.base_url = self.get_url_endpoint()

    def get_products(self, state) -> Tuple[List, int]:
        """Returns index for sync resuming on interruption."""
        shared_product_ids = Products(self.client).prefetch_product_ids()
        last_synced = get_bookmark(state, self.tap_stream_id, "currently_syncing", False)
        last_sync_index = 0
        if last_synced:
            for pos, (prod_id, _) in enumerate(shared_product_ids):
                if prod_id == last_synced:
                    LOGGER.warning("Last Sync was interrupted after product *****%s", str(prod_id)[-4:])
                    last_sync_index = pos
                    break
        return shared_product_ids, last_sync_index

    def get_records(self, prod_id: str, bookmark_date: str) -> Tuple[List, datetime]:
        # pylint: disable=W0221
        """Performs api querying and pagination of response.

        Retrieves all record and filters within the code, as the API
        endpoint does not have any query parameter to fetch the latest
        record from specific date.
        """
        extraction_url = self.base_url.replace("PRODUCT_ID", prod_id)
        bookmark_date = current_max = strptime_to_utc(bookmark_date)
        filtered_records = []
        page_count, params = 1, {}
        while True:
            LOGGER.info("Calling Page %s", page_count)

            response = self.client.get(extraction_url, params, {}, self.api_auth_version)

            # response = response.get("response", {})
            raw_records = response.get("variants", [])
            pagination = response.get("pagination", {}).get("next_page_info", None)

            if not raw_records:
                break

            for record in raw_records:
                record_timestamp = strptime_to_utc(record[self.replication_key])
                if record_timestamp >= bookmark_date:
                    current_max = max(current_max, record_timestamp)

                    # Adding yotpo_product_id in record
                    if "yotpo_product_id" not in record.keys():
                        record["yotpo_product_id"] = int(prod_id)
                    filtered_records.append(record)

            if not pagination:
                break
            else:
                params["page_info"] = pagination
            page_count += 1

        return (filtered_records, current_max)

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Sync implementation for `product_variants` stream."""
        # pylint: disable=R0914
        with metrics.Timer(self.tap_stream_id, None):
            config_start = self.client.config[self.config_start_key]
            products, start_index = self.get_products(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            prod_len = len(products)

            with metrics.Counter(self.tap_stream_id) as counter:
                # pylint: disable=W0612
                for index, (prod_id, ext_prod_id) in enumerate(products[start_index:], max(start_index, 1)):

                    LOGGER.info("Sync for prod *****%s (%s/%s)", str(prod_id)[-4:], index, prod_len)

                    bookmark_date = get_bookmark(state, self.tap_stream_id, str(prod_id), config_start)
                    records, max_bookmark = self.get_records(str(prod_id), bookmark_date)

                    for _ in records:
                        write_record(self.tap_stream_id, transformer.transform(_, schema, stream_metadata))
                        counter.increment()

                    # bookmark value won't be updated for those prod_id which are not having any latest
                    # variants records.
                    if records:
                        state = self.write_bookmark(state, str(prod_id), strftime(max_bookmark))
                    state = self.write_bookmark(state, "currently_syncing", str(prod_id))
                    write_state(state)
            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")
        return state