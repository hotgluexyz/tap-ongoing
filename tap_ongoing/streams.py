"""Stream type classes for tap-ongoing."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_ongoing.client import ongoingStream


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ArticlesStream(ongoingStream):
    name = "articles"
    path = "articles"
    primary_keys = ["articleSystemId"]
    replication_key = None
    schema_filepath = f"{SCHEMAS_DIR}/articles.json"


class OrdersStream(ongoingStream):
    """Define custom stream."""

    name = "orders"
    path = "orders"
    records_jsonpath = "$[*].orderInfo"
    primary_keys = ["orderId"]
    replication_key = None
    schema_filepath = f"{SCHEMAS_DIR}/orders.json"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["goodsOwnerId"] = self.config["goods_owner_id"]
        params["orderCreatedTimeFrom"] = self.config["start_date"]
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params
