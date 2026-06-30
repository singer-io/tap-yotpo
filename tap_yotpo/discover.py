"""tap-yotpo discover module."""
import json

from singer import get_logger
from singer.catalog import Catalog

from tap_yotpo.exceptions import Http403RequestError
from tap_yotpo.helpers import get_abs_path
from tap_yotpo.streams import STREAMS

LOGGER = get_logger()


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """Remove child streams whose parent was excluded from schemas."""
    for name, stream_cls in list(STREAMS.items()):
        if name in schemas and stream_cls.parent and stream_cls.parent not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                name,
                stream_cls.parent,
            )
            schemas.pop(name, None)
            field_metadata.pop(name, None)


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """Probe each stream for read access and remove inaccessible streams in place."""
    inaccessible_streams = [
        stream_name
        for stream_name, stream_cls in STREAMS.items()
        if stream_name in schemas and not stream_cls(client=client).check_access()
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise Http403RequestError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams."
        )
    elif inaccessible_streams:
        LOGGER.warning(
            "No 'read' access to stream(s): %s. Excluded from catalog.",
            ", ".join(inaccessible_streams),
        )


def discover(client) -> Catalog:
    """Performs Discovery for tap-yotpo."""
    schemas = {}
    field_metadata = {}
    for stream_name, stream_cls in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        with open(schema_path, encoding="utf-8") as schema_file:
            schemas[stream_name] = json.load(schema_file)
        field_metadata[stream_name] = stream_cls.get_metadata(schemas[stream_name])

    _apply_access_checks(client, schemas, field_metadata)

    streams = [
        {
            "stream": stream_name,
            "tap_stream_id": STREAMS[stream_name].tap_stream_id,
            "schema": schema,
            "metadata": field_metadata[stream_name],
        }
        for stream_name, schema in schemas.items()
    ]
    return Catalog.from_dict({"streams": streams})
