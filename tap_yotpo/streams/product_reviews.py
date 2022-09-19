"""tap-yotpo product-reviews stream module."""
from datetime import datetime
from math import ceil
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

from tap_yotpo.helpers import ApiSpec, skip_product

from .abstracts import IncrementalStream, UrlEndpointMixin,PageSizeMixin
from .products import Products

LOGGER = get_logger()


class ProductReviews(IncrementalStream, UrlEndpointMixin,PageSizeMixin):
    """class for product_reviews stream."""

    stream = "product_reviews"
    tap_stream_id = "product_reviews"
    key_properties = ["id"]
    replication_key = "created_at"
    valid_replication_keys = ["created_at"]
    api_auth_version = ApiSpec.API_V1
    # points to the attribute of the config that marks the first-start-date for the stream
    config_start_key = "start_date"
    url_endpoint = "https://api-cdn.yotpo.com/v1/widget/APP_KEY/products/PRODUCT_ID/reviews.json"
    default_page_size = 150

    def __init__(self, client=None) -> None:
        super().__init__(client)
        self.base_url = self.get_url_endpoint()

    def get_products(self, state: Dict) -> Tuple[List, int]:
        """Returns index for sync resuming on interuption."""
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

    def get_records(
        self, product__external_id: str, product__yotpo_id: str, bookmark_date: str
    ) -> Tuple[List, datetime]:
        # pylint: disable=W0221
        """performs api querying and pagination of response."""
        params = {"page": 1, "per_page": self.page_size, "sort": "date", "direction": "desc"}
        extraction_url = self.base_url.replace("PRODUCT_ID", product__external_id)
        config_start = self.client.config.get(self.config_start_key, False)
        bookmark_date = current_max = max(strptime_to_utc(bookmark_date), strptime_to_utc(config_start))
        next_page, prod_map = True, None
        filtered_records = []
        while next_page:
            response = self.client.get(extraction_url, params, {}, self.api_auth_version)
            response = response.get("response", {})
            raw_records = response.get("reviews", [])
            if not prod_map:
                prod_map = {px["id"]: px["name"] for px in response.get("products", [])}
            current_page = response.get("pagination", {}).get("page", None)
            total_records = response.get("pagination", {}).get("total", None)
            max_pages = max(ceil(total_records / params["per_page"]), 1)

            if not raw_records:
                break

            LOGGER.info("Page: (%s/%s)", current_page, max(ceil(total_records / 150), 1))

            for record in raw_records:
                record_timestamp = strptime_to_utc(record[self.replication_key])
                if record_timestamp >= bookmark_date:
                    try:
                        current_max = max(current_max, record_timestamp)
                        record["domain_key"] = product__external_id
                        record["product_yotpo_id"] = product__yotpo_id
                        record["name"] = prod_map[record["product_id"]]
                        filtered_records.append(record)
                    except KeyError as _:
                        LOGGER.fatal("Error: %s for prod_id %s ", str(_), product__yotpo_id[-4:])
                else:
                    next_page = False

            params["page"] += 1

            if params["page"] > max_pages:
                next_page = False

        return (filtered_records, current_max, total_records)

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Sync implementation for `product_reviews` stream."""
        # pylint: disable=R0914
        with metrics.Timer(self.tap_stream_id, None):
            products, start_index = self.get_products(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            prod_len = len(products)

            with metrics.Counter(self.tap_stream_id) as counter:
                for index, (product__yotpo_id, product__external_id) in enumerate(
                    products[start_index:], max(start_index, 1)
                ):
                    product__yotpo_id = str(product__yotpo_id)
                    if skip_product(product__external_id):
                        LOGGER.info(
                            "Skipping Prod *****%s (%s/%s),Can't fetch reviews for products with special characters %s",
                            product__yotpo_id[-4:],
                            index,
                            prod_len,
                            product__external_id,
                        )
                        continue

                    LOGGER.info("Sync for prod *****%s (%s/%s)", product__yotpo_id[-4:], index, prod_len)

                    bookmark_date = self.get_bookmark(state, product__yotpo_id)
                    records, max_bookmark, total_records = self.get_records(
                        product__external_id, product__yotpo_id, bookmark_date
                    )

                    for _ in records:
                        write_record(self.tap_stream_id, transformer.transform(_, schema, stream_metadata))
                        counter.increment()

                    LOGGER.info("Total records : %s, Total records synced : %s", total_records, len(records))
                    state = self.write_bookmark(state, product__yotpo_id, strftime(max_bookmark))
                    state = self.write_bookmark(state, "currently_syncing", product__yotpo_id)
                    write_state(state)
            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")
        return state
