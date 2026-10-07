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

from tap_yotpo import exceptions as errors
from tap_yotpo.helpers import ApiSpec

from .abstracts import IncrementalStream, PageSizeMixin, UrlEndpointMixin
from .products import Products

LOGGER = get_logger()


class ProductVariants(IncrementalStream, UrlEndpointMixin, PageSizeMixin):
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
    parent = "products"
    # safety bound so a misbehaving pagination cursor cannot loop forever
    max_pages = 1000

    def __init__(self, client=None) -> None:
        super().__init__(client)
        self.base_url = self.get_url_endpoint()

    def get_products(self, state: Dict) -> Tuple[List, int]:
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
        page_count, params = 1, {"limit": self.page_size}
        has_more_pages = True
        while has_more_pages and page_count <= self.max_pages:
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
                has_more_pages = False
            else:
                params["page_info"] = pagination
                page_count += 1

        if page_count > self.max_pages:
            LOGGER.warning(
                "Reached the max page limit of %s for product *****%s; stopping pagination",
                self.max_pages,
                prod_id[-4:],
            )

        return (filtered_records, current_max)

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Sync implementation for `product_variants` stream."""
        # pylint: disable=R0914
        with metrics.Timer(self.tap_stream_id, None):
            config_start = self.client.config[self.config_start_key]
            products, start_index = self.get_products(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            prod_len = len(products)
            unavailable_products = []

            with metrics.Counter(self.tap_stream_id) as counter:
                # pylint: disable=W0612
                for index, (prod_id, ext_prod_id) in enumerate(products[start_index:], max(start_index, 1)):

                    LOGGER.info("Sync for prod *****%s (%s/%s)", str(prod_id)[-4:], index, prod_len)

                    bookmark_date = get_bookmark(state, self.tap_stream_id, str(prod_id), config_start)
                    try:
                        records, max_bookmark = self.get_records(str(prod_id), bookmark_date)
                    except (errors.Http404RequestError, errors.Http500RequestError) as exc:
                        # Yotpo can serve a deterministic 500 for the variants sub-resource of a
                        # product even though the parent product itself is readable, and a 404 when
                        # a product is removed between the parent prefetch and this request. Retries
                        # can never succeed, so failing here would permanently block the stream and
                        # discard every remaining product. Leave the bookmark untouched so the
                        # product is retried on the next sync.
                        LOGGER.error(
                            "Product *****%s (%s/%s): Yotpo returned a persistent error for %s - %s. "
                            "Skipping this product; its bookmark is left unchanged so it is retried "
                            "on the next sync.",
                            str(prod_id)[-4:],
                            index,
                            prod_len,
                            self.base_url.replace("PRODUCT_ID", str(prod_id)),
                            exc,
                        )
                        unavailable_products.append(prod_id)
                        state = self.write_bookmark(state, "currently_syncing", str(prod_id))
                        write_state(state)
                        continue

                    for _ in records:
                        write_record(self.tap_stream_id, transformer.transform(_, schema, stream_metadata))
                        counter.increment()

                    # bookmark value won't be updated for those prod_id which are not having any latest
                    # variants records.
                    if records:
                        state = self.write_bookmark(state, str(prod_id), strftime(max_bookmark))
                    state = self.write_bookmark(state, "currently_syncing", str(prod_id))
                    write_state(state)

            if unavailable_products:
                LOGGER.warning(
                    "%s of %s products were skipped because the Yotpo variants endpoint "
                    "returned a persistent error for them: %s",
                    len(unavailable_products),
                    prod_len,
                    ", ".join("*****{}".format(str(_id)[-4:]) for _id in unavailable_products),
                )
            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")
        return state
