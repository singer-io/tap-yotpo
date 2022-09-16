"""
Test tap pagination of streams
"""

from math import ceil
from tap_tester import runner, connections
from base import YotpoBaseTest


class YotpoPaginationTest(YotpoBaseTest):
    """Checking the pagination is working properly or not for the streams supporting pagination"""

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.streams_to_test = None
        self.page_size = None

    @staticmethod
    def name():
        return "tap_tester_yotpo_pagination_test"

    def get_properties(self, original: bool = False):
        """Configuration of properties required for the tap."""
        return_value = {
            'start_date': '2021-01-01T00:00:00Z',
            'email_stats_lookback_days': 30,
            'reviews_lookback_days': 30
        }
        if original:
            return return_value

        return_value["page_size"] = self.page_size
        return return_value

    def validate_pagination(self, page_size, stream_records):
        """
        Verify records are more than page size so multiple page is working
        Chunk the replicated records (just primary keys) into expected pages
        """
        record_count = len(stream_records)
        if record_count < page_size:
            return True

        pages = []
        page_count = ceil(len(stream_records) / page_size)
        for page_index in range(page_count):
            page_start = page_index * page_size
            page_end = (page_index + 1) * page_size
            pages.append(set(stream_records[page_start:page_end]))

        # Verify by primary keys that data is unique for each page
        for current_index, current_page in enumerate(pages):
            with self.subTest(current_page_primary_keys=current_page):

                for other_index, other_page in enumerate(pages):
                    if current_index == other_index:
                        continue  # don't compare the page to itself

                    # Verify there are no duplicates between pages
                    self.assertTrue(
                        current_page.isdisjoint(other_page), msg=f'other_page_primary_keys={other_page}')
        return True

    def test_run(self):
        """Executing run_test with different page_size values for different streams"""
        
        # Skipping streams emails and unsubscribers because of insufficient test data
        testable_streams = self.expected_streams() - {"emails", "unsubscribers"}
        
        # Depending on test data data availability passing page_size value to verify pagination implementation
        self.run_test(testable_streams - {"product_reviews", "product_variants", "order_fulfillments"}, 20)
        self.run_test({"product_reviews", "product_variants", "order_fulfillments"}, 50)

    def run_test(self, expected_streams, page_size):
        """Checking pagination for streams with enough data"""
        self.streams_to_test = expected_streams
        self.page_size = page_size

        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # Expected values
                expected_primary_keys = self.expected_primary_keys()

                # Collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync = record_count_by_stream.get(stream, 0)
                stream_records = [tuple(message.get('data')
                                        for expected_pk in expected_primary_keys[stream])
                                  for message in synced_records.get(stream).get('messages')
                                  if message.get('action') == 'upsert']

                # Verify records are more than page size so multiple page is working
                self.assertGreater(record_count_sync, page_size)

                if stream in ["product_reviews", "order_fulfillments", "product_variants"]:
                    current_product_id = None
                    current_product_id_records = []
                    pagination_records_found = False
                    parent_keys = {
                        "product_reviews": ("id", "product_yotpo_id"),
                        "order_fulfillments": ("id", "order_id"),
                        "product_variants": ("yotpo_id", "yotpo_product_id")
                    }[stream]

                    for record in stream_records:
                        primary_key, parent_id = record[0][parent_keys[0]], record[0][parent_keys[1]]
                        if current_product_id == parent_id:
                            current_product_id_records.append((primary_key, parent_id))
                        else:
                            pagination_records_found = pagination_records_found or len(current_product_id_records) > page_size
                            self.assertTrue(
                                self.validate_pagination(page_size, current_product_id_records))
                            current_product_id = parent_id
                            current_product_id_records = [(primary_key, parent_id)]

                    pagination_records_found = pagination_records_found or len(current_product_id_records) > page_size

                    self.assertTrue(
                        self.validate_pagination(page_size, current_product_id_records))

                    self.assertTrue(
                        pagination_records_found,
                        msg=f"Not enough test data, either add more test data or reduce the page_size={page_size}")
                else:
                    # Expected values
                    expected_primary_keys = self.expected_primary_keys()

                    # Collect information for assertions from syncs 1 & 2 base on expected values
                    record_count_sync = record_count_by_stream.get(stream, 0)
                    primary_keys_list = [tuple(message.get('data').get(expected_pk)
                                            for expected_pk in expected_primary_keys[stream])
                                        for message in synced_records.get(stream).get('messages')
                                        if message.get('action') == 'upsert']

                    self.assertGreater(
                        record_count_sync, page_size, msg=f"Not enough test data, either add more test data or reduce the page_size={page_size}")

                    self.assertTrue(
                        self.validate_pagination(page_size, primary_keys_list))
