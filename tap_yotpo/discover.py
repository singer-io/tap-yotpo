"""tap-yotpo discover module."""
import json
from typing import Dict

from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

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
            schema_dict = json.load(schema_file)

        stream_metadata = stream.get_metadata(schema_dict)
        root_metadata_entry = next(
            (entry for entry in stream_metadata if entry.get("breadcrumb") in ((), [])),
            None,
        )
        if root_metadata_entry is not None:
            root_metadata = root_metadata_entry.setdefault("metadata", {})
            root_metadata.setdefault(
                "replication-method",
                stream.replication_method or stream.forced_replication_method,
            )
            if getattr(stream, "parent", ""):
                root_metadata["parent-tap-stream-id"] = stream.parent

        streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream.tap_stream_id,
                key_properties=list(stream.key_properties),
                schema=Schema.from_dict(schema_dict),
                metadata=stream_metadata,
            )
        )
    return Catalog(streams)
