#!/usr/bin/env python3
import os
import singer
from singer import utils #, metadata
from singer.catalog import Catalog #, CatalogEntry, Schema
#from . import streams as streams_
from .context import Context

REQUIRED_CONFIG_KEYS = ["start_date", "api_key", "api_secret"]
LOGGER = singer.get_logger()


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
