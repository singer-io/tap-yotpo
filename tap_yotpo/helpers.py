"""tap-yotpo helper functions module"""
import enum
import os
import re

from singer import get_logger

LOGGER = get_logger()


def skip_product(prod_id) -> bool:
    """
    checks for special chars in product_name.
    """
    return not bool(re.match("^[A-Za-z0-9_-]*$", prod_id))


def get_abs_path(path: str) -> str:
    """
    Returns absolute path for URL
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def wait_gen():
    """
    Returns a generator that is passed to backoff decorator to indicate how long
    to backoff for in seconds.
    """
    while True:
        LOGGER.info("API exception occured sleeping for 60 seconds")
        yield 60


class ApiSpec(enum.Enum):
    """
    Representing standard APi version mappings
    """

    API_V1 = enum.auto()
    API_V3 = enum.auto()
