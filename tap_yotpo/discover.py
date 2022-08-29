"""tap-yotpo discover module."""
import json

from singer.catalog import Catalog

from tap_yotpo.helpers import get_abs_path, ApiSpec
from .streams import STREAMS

from tap_yotpo.client import Client


def discover(config = None):
    """
    TODO: Permission Check
    """
    if config :
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
