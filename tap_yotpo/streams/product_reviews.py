"""tap-yotpo product-reviews stream module"""
from datetime import datetime
from math import ceil
from typing import Dict, List, Tuple

import singer
from singer import (
    Transformer,
    clear_bookmark,
    get_bookmark,
    metrics,
    write_bookmark,
    write_record,
    write_state,
)
from singer.utils import strptime_to_utc

from tap_yotpo.helpers import ApiSpec, skip_product

from .abstracts import IncremetalStream
from .products import Products

LOGGER = singer.get_logger()


class ProductReviews(IncremetalStream):
    """
    class for product_reviews stream
    """

    stream = "product_reviews"
    tap_stream_id = "product_reviews"
    key_properties = [
        "id",
    ]
    replication_key = "created_at"
    valid_replication_keys = ["created_at"]
    api_auth_version = ApiSpec.API_V1
    config_start_key = "start_date"
    url_endpoint = " https://api-cdn.yotpo.com/v1/widget/APP_KEY/products/PRODUCT_ID/reviews.json"

    def __init__(self, client=None) -> None:
        super().__init__(client)
        self.sync_prod: bool = True
        self.last_synced: bool = False
        self.base_url = self.get_url_endpoint()

    def get_url_endpoint(self) -> str:
        """
        Returns a formated endpoint using the stream attributes
        """
        return self.url_endpoint.replace("APP_KEY", self.client.config["api_key"])

    def get_products(self, state) -> Tuple[List, int]:
        """
        Returns index for sync resuming on interuption
        """
        shared_product_ids = Products(self.client).prefetch_product_ids()
        last_synced = singer.get_bookmark(state, self.tap_stream_id, "currently_syncing", False)
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
        """
        performs api querying and pagination of response
        """
        params = {"page": 1, "per_page": 150, "sort": ["date", "time"], "direction": "desc"}
        extraction_url = self.base_url.replace("PRODUCT_ID", prod_id)
        bookmark_date = current_max = strptime_to_utc(bookmark_date)
        next_page = True
        filtered_records = []
        while next_page:

            response = self.client.get(extraction_url, params, {}, self.api_auth_version)

            response = response.get("response", {})
            raw_records = response.get("reviews", [])
            current_page = response.get("pagination", {}).get("page", None)
            total_records = response.get("pagination", {}).get("total", None)
            max_pages = max(ceil(total_records / 150), 1)

            if not raw_records:
                break

            LOGGER.info(
                "Page: (%s/%s) Total Records: %s", current_page, max(ceil(total_records / 150), 1), total_records
            )

            for record in raw_records:
                record_timestamp = strptime_to_utc(record[self.replication_key])
                if record_timestamp >= bookmark_date:
                    current_max = max(current_max, record_timestamp)
                    record["domain_key"] = prod_id
                    filtered_records.append(record)
                else:
                    next_page = False

            params["page"] += 1

            if params["page"] > max_pages:
                next_page = False

        return (filtered_records, current_max)

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """
        Sync implementation for `product_reviews` stream
        """
        # pylint: disable=R0914
        with metrics.Timer(self.tap_stream_id, None):
            config_start = self.client.config[self.config_start_key]
            products, start_index = self.get_products(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            prod_len = len(products)
            with metrics.Counter(self.tap_stream_id) as counter:
                for index, (prod_id, ext_prod_id) in enumerate(products[start_index:], max(start_index, 1)):

                    if skip_product(ext_prod_id):
                        LOGGER.info(
                            "Skipping Prod *****%s (%s/%s),Cant fetch reviews for products with special charecters %s",
                            str(prod_id)[-4:],
                            index,
                            prod_len,
                            ext_prod_id,
                        )
                        continue

                    LOGGER.info("Sync for prod *****%s (%s/%s)", str(prod_id)[-4:], index, prod_len)

                    old_bmk = get_bookmark(state, self.tap_stream_id, str(prod_id), config_start)
                    records, current_bmk = self.get_records(ext_prod_id, old_bmk)

                    for _ in records:
                        write_record(self.tap_stream_id, transformer.transform(_, schema, stream_metadata))
                        counter.increment()

                    state = write_bookmark(
                        state, self.tap_stream_id, prod_id, current_bmk.strftime("%Y-%m-%dT%H:%M:%SZ")
                    )
                    state = write_bookmark(state, self.tap_stream_id, "currently_syncing", prod_id)
                    write_state(state)
            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")
        return state
