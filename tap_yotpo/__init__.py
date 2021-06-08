#!/usr/bin/env python3
import os
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams as streams_
from .context import Context

REQUIRED_CONFIG_KEYS = ["start_date", "api_key", "api_secret"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(ctx, tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {}
    for sub_stream_id in dependencies:
        refs[sub_stream_id] = load_schema(ctx, sub_stream_id)
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def load_and_write_schema(ctx, stream):
    singer.write_schema(
        stream.tap_stream_id,
        load_schema(ctx, stream.tap_stream_id),
        stream.pk_fields,
    )


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

        catalog.streams.append(CatalogEntry(
            stream=stream.tap_stream_id,
            tap_stream_id=stream.tap_stream_id,
            key_properties=stream.pk_fields,
            schema=schema,
            metadata=metadata.to_list(mdata)
        ))
    return catalog


def sync(ctx):
    streams_.products.fetch_into_cache(ctx)

    currently_syncing = ctx.state.get("currently_syncing")
    start_idx = streams_.all_stream_ids.index(currently_syncing) \
        if currently_syncing else 0
    streams = [s for s in streams_.all_streams[start_idx:]
               if s.tap_stream_id in ctx.selected_stream_ids]
    for stream in streams:
        ctx.state["currently_syncing"] = stream.tap_stream_id
        ctx.write_state()
        load_and_write_schema(ctx, stream)
        stream.sync(ctx)
    ctx.state["currently_syncing"] = None
    ctx.write_state()


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    ctx = Context(args.config, args.state)
    ctx.client.authenticate()
    if args.discover:
        discover(ctx).dump()
        print()
    else:
        ctx.catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover(ctx)
        sync(ctx)


if __name__ == "__main__":
    main()
