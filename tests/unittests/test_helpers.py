import enum
from unittest import TestCase

from tap_yotpo.client import Client
from tap_yotpo.helpers import skip_product, wait_gen


class TestHelperMethods(TestCase):
    """Checking all helper methods for streams."""

    client_obj = Client({"api_key": enum.auto(), "api_secret": enum.auto()})

    def test_product_filter(self, *args):
        """Test the `skip_product` function This function checks if the
        `product_ids` are urlsafe and will not break the extraction."""
        valid_prod_ids = ["67776", "TestProd", "Alp4aN7M"]
        invalid_prod_ids = ["675675:#87", "(*&Invalid2", "EndsWith*&@"]
        for _ in valid_prod_ids:
            self.assertEqual(False, skip_product(_))
        for _ in invalid_prod_ids:
            self.assertEqual(True, skip_product(_))

    def test_waitgen(self, *args):
        """Test the WaitGen if it returns `60` seconds as wait time."""
        self.assertEqual(60, wait_gen().__next__())
