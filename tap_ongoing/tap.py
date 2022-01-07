"""ongoing tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_ongoing.streams import (
    ArticlesStream,
    InventoryAdjustmentsStream,
    PurchaseOrdersStream,
    OrdersStream,
    ArticleItemsStream
)

STREAM_TYPES = [
    ArticlesStream,
    InventoryAdjustmentsStream,
    PurchaseOrdersStream,
    OrdersStream,
    ArticleItemsStream
]


class Tapongoing(Tap):
    """ongoing tap class."""
    name = "tap-ongoing"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="The API username"
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="The API password"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
        th.Property(
            "goods_owner_id",
            th.StringType,
            default="https://api.mysample.com",
            description="The goods owner id"
        ),
        th.Property(
            "warehouse_name",
            th.StringType,
            default="https://api.mysample.com",
            description="The warehouse name"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

if __name__=="__main__":
    Tapongoing.cli()