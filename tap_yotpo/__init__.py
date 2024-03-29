"""tap-yotpo module."""
import sys

from singer import get_logger, utils
from singer.metrics import Counter, Timer

from tap_yotpo.client import Client
from tap_yotpo.discover import discover
from tap_yotpo.sync import sync

REQUIRED_CONFIG_KEYS = ["start_date", "api_key", "api_secret"]
LOGGER = get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    """performs sync and discovery."""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    client = Client(args.config)
    if args.discover:
        discover(args.config).dump()
    else:
        with Counter("total_requests", log_interval=sys.maxsize) as req_counter, Timer(
            "total_extraction_time", None
        ) as req_timer:
            client.req_counter = req_counter
            client.req_timer = req_timer
            sync(client, args.catalog or discover(args.config), args.state)


if __name__ == "__main__":
    main()
