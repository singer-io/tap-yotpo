import singer
from singer import utils
from singer.catalog import Catalog
from tap_yotpo.discover import discover
from tap_yotpo.sync import sync
from tap_yotpo.context import Context
from tap_yotpo.client import Client

REQUIRED_CONFIG_KEYS = ["start_date", "api_key", "api_secret"]
LOGGER = singer.get_logger()


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    client = Client(args.config)
    if args.discover:
        discover(client).dump()
    else:
        catalog = Catalog.from_dict(args.properties) if args.properties else discover(client)
        sync(client,catalog,args.state)


if __name__ == "__main__":
    main()
