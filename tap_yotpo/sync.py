from . import streams as streams_
from tap_yotpo.helpers import load_and_write_schema

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