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
            inaccessible_streams.append(stream_name)

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    inaccessible_streams.extend(_prune_inaccessible_children(schemas, field_metadata))

    if not schemas:
        raise Http403RequestError(
            "No streams are accessible. Ensure the credentials have read permission for at least one stream."
        )
    
    if inaccessible_streams:
        LOGGER.warning(
            "Unauthorized streams excluded from catalog: %s",
            ", ".join(inaccessible_streams),
        )


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """Remove child streams from the catalog whose parent stream was excluded.

    Mutates schemas and field_metadata in place.
    """
    to_remove = []
    for stream_name, stream_class in list(STREAMS.items()):
        parent = getattr(stream_class, "parent", "")
        if stream_name in schemas and parent and parent not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                stream_name,
                parent,
            )
            schemas.pop(stream_name, None)
            field_metadata.pop(stream_name, None)
            to_remove.append(stream_name)
    return to_remove


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
