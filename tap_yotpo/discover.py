"""tap-yotpo discover module."""
import json
from typing import Dict

from singer.catalog import Catalog

from tap_yotpo.client import Client
from tap_yotpo.helpers import ApiSpec, get_abs_path
from tap_yotpo.streams import STREAMS


def discover(config: Dict = None):
    """Performs Discovery for tap-yotpo."""
    if config:
        client = Client(config)
        client.authenticate({}, {}, ApiSpec.API_V3)
    streams = []
    for stream_name, stream in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        with open(schema_path, encoding="utf-8") as schema_file:
            schema = json.load(schema_file)
        streams.append(
            {
                "stream": stream_name,
                "tap_stream_id": stream.tap_stream_id,
                "schema": schema,
                "metadata": stream.get_metadata(schema),
            }
        )
    return Catalog.from_dict({"streams": streams})
