"""tap-yotpo discover module."""
import json

from singer import get_logger
from singer.catalog import Catalog

from tap_yotpo.exceptions import Http403RequestError
from tap_yotpo.helpers import get_abs_path
from tap_yotpo.streams import STREAMS

LOGGER = get_logger()


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """Checks accessibility of each stream, removing inaccessible ones.

    Streams that return a 403 Forbidden response are excluded from the catalog.
    After removing inaccessible parent streams, child streams whose parents were
    excluded are also pruned. Raises if no parent streams remain accessible.
    """
    inaccessible_streams = []
    for stream_name, stream_class in list(schemas.items()):
        stream_instance = stream_class(client)
        if not stream_instance.check_access():
            LOGGER.warning(
                "Stream '%s' is not accessible (403 Forbidden). Excluding from catalog.",
                stream_name,
            )
            inaccessible_streams.append(stream_name)

    for stream_name in inaccessible_streams:
        del schemas[stream_name]
        del field_metadata[stream_name]

    _prune_inaccessible_children(schemas, field_metadata)

    accessible_parents = [s for s in schemas.values() if not getattr(s, "parent", "")]
    if not accessible_parents:
        raise Exception("All parent streams are inaccessible. Cannot produce a usable catalog.")


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """Removes child streams whose parents have been excluded from schemas."""
    to_remove = []
    for stream_name, stream_class in schemas.items():
        parent = getattr(stream_class, "parent", "")
        if parent and parent not in schemas:
            LOGGER.warning(
                "Stream '%s' is a child of '%s' which is excluded. Excluding child stream too.",
                stream_name,
                parent,
            )
            to_remove.append(stream_name)

    for stream_name in to_remove:
        del schemas[stream_name]
        del field_metadata[stream_name]

    # Recurse to handle multi-level parent-child relationships
    if to_remove:
        _prune_inaccessible_children(schemas, field_metadata)


def discover(client) -> Catalog:
    """Performs Discovery for tap-yotpo."""
    schemas = {}
    field_metadata = {}

    for stream_name, stream_class in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        with open(schema_path, encoding="utf-8") as schema_file:
            schema = json.load(schema_file)
        schemas[stream_name] = stream_class
        field_metadata[stream_name] = (schema, stream_class.get_metadata(schema))

    _apply_access_checks(client, schemas, field_metadata)

    streams = []
    for stream_name, stream_class in schemas.items():
        schema, metadata = field_metadata[stream_name]
        streams.append(
            {
                "stream": stream_name,
                "tap_stream_id": stream_class.tap_stream_id,
                "schema": schema,
                "metadata": metadata,
            }
        )
    return Catalog.from_dict({"streams": streams})
