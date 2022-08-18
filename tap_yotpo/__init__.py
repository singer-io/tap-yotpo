"""tap-yotpo module."""
import singer
from singer import utils

from tap_yotpo.client import Client
from tap_yotpo.discover import discover
from tap_yotpo.sync import sync

REQUIRED_CONFIG_KEYS = ["start_date", "api_key", "api_secret"]
LOGGER = singer.get_logger()


@singer.utils.handle_top_exception(LOGGER)
def main():
    """performs sync and discovery."""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    client = Client(args.config)
    if args.discover:
        discover(client).dump()
    else:
        sync(client, args.catalog or discover(args.config), args.state)


if __name__ == "__main__":
    main()
