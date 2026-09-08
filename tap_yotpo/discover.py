"""tap-yotpo discover module."""
import json
from typing import Dict

from singer import get_logger
from singer.catalog import Catalog

from tap_yotpo.client import Client
from tap_yotpo.helpers import ApiSpec, get_abs_path
from tap_yotpo.streams import STREAMS

LOGGER = get_logger()


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

        stream_metadata = stream.get_metadata(schema)
        root_metadata = {}
        for entry in stream_metadata:
            if entry.get("breadcrumb") in ((), []):
                root_metadata = entry.get("metadata", {})
                break

        key_properties = root_metadata.get("table-key-properties") or list(stream.key_properties)

        LOGGER.info(
            "discover stream=%s tap_stream_id=%s keys=%s root_metadata=%s",
            stream_name,
            stream.tap_stream_id,
            list(schema.get("properties", {}).keys()),
            root_metadata,
        )

        streams.append(
            {
                "stream": stream_name,
                "tap_stream_id": stream.tap_stream_id,
                "key_properties": key_properties,
                "schema": schema,
                "metadata": stream_metadata,
            }
        )
    return Catalog.from_dict({"streams": streams})
