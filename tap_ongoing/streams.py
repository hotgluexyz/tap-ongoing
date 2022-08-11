"""Stream type classes for tap-ongoing."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th
import requests

from tap_ongoing.client import ongoingStream


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class ArticlesStream(ongoingStream):
    name = "articles"
    path = "articles"
    primary_keys = ["articleSystemId"]
    replication_key = None
    schema_filepath = f"{SCHEMAS_DIR}/articles.json"

class PurchaseOrdersStream(ongoingStream):
    name = "purchase orders"
    path = "purchaseOrders"
    primary_keys = ["purchaseOrderId"]
    replication_key = None
    schema_filepath = f"{SCHEMAS_DIR}/purchaseOrders.json"
    stream_params = {"purchaseOrderStatusFrom": 0}

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for item in response.json():
            output = item.get("purchaseOrderInfo")
            output["purchaseOrderLines"] = item.get("purchaseOrderLines")
            yield output

class OrdersStream(ongoingStream):
    name = "orders"
    path = "orders"
    records_jsonpath = "$[*].orderInfo"
    primary_keys = ["orderId"]
    replication_key = None
    schema_filepath = f"{SCHEMAS_DIR}/orders.json"
    paginate = {"start": "orderCreatedTimeFrom", "end": "orderCreatedTimeTo"}

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for item in response.json():
            output = item.get("orderInfo")
            output["orderLines"] = item.get("orderLines")
            yield output


class ArticleItemsStream(ongoingStream):
    name = "article items"
    path = "ArticleItems"
    records_jsonpath = "$[*]"
    primary_keys = ["articleSystemId"]
    replication_key = None
    schema_filepath = f"{SCHEMAS_DIR}/ArticleItems.json"
    stream_params = {"articleSystemIdFrom": 0}
