import json

from singer.catalog import Catalog

from tap_yotpo.helpers import get_abs_path
from tap_yotpo.streams import STREAMS

# def get_schemas() -> tuple[dict,dict]:
#     """
#     Builds the singer schema and metadata dictionaries.
#     """


def discover(client):
    """
    TODO: Permission Check
    """
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
