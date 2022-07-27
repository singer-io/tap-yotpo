from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams as streams_
from helpers import load_schema


def discover(ctx):
    catalog = Catalog([])
    for stream in streams_.all_streams:
        schema_dict = load_schema(ctx, stream.tap_stream_id)
        schema = Schema.from_dict(schema_dict)
        mdata = metadata.get_standard_metadata(schema_dict,
                                               key_properties=stream.pk_fields)
        mdata = metadata.to_map(mdata)

        for field_name in schema_dict['properties'].keys():
            # Field except primary keys and book mark keys marked as available. 
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion',
                                   'automatic' if field_name in stream.pk_fields or field_name in stream.book_mark_keys else 'available')

        catalog.streams.append(CatalogEntry(stream=stream.tap_stream_id,
                                           tap_stream_id=stream.tap_stream_id,
                                           key_properties=stream.pk_fields,
                                           schema=schema,
                                           metadata=metadata.to_list(mdata)))
    return catalog