import os
import re
import singer
from singer import utils

def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")

def skip_product(prod_id) -> bool:
        return True if re.match("^[A-Za-z0-9_-]*$", prod_id) is None else False


def get_abs_path(path :str) -> str:
    """
    Returns absolute path for URL
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
