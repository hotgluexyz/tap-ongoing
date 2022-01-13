"""Stream type classes for tap-ongoing."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
import requests

from tap_ongoing.client import ongoingStream
from singer_sdk.helpers.jsonpath import extract_jsonpath


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

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for item in response.json():
            output = item.get("purchaseOrderInfo")
            output["purchaseOrderLines"] = item.get("purchaseOrderLines")
            yield output

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["goodsOwnerId"] = self.config["goods_owner_id"]
        params["purchaseOrderStatusFrom"] = 0
        if next_page_token:
            params["page"] = next_page_token
        return params

class OrdersStream(ongoingStream):
    name = "orders"
    path = "orders"
    records_jsonpath = "$[*].orderInfo"
    primary_keys = ["orderId"]
    replication_key = None
    schema_filepath = f"{SCHEMAS_DIR}/orders.json"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        for item in response.json():
            output = item.get("orderInfo")
            output["orderLines"] = item.get("orderLines")
            yield output

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["goodsOwnerId"] = self.config["goods_owner_id"]
        params["orderCreatedTimeFrom"] = self.config["start_date"]
        if next_page_token:
            params["page"] = next_page_token
        return params


class ArticleItemsStream(ongoingStream):
    name = "article items"
    path = "ArticleItems"
    records_jsonpath = "$[*]"
    primary_keys = ["articleSystemId"]
    replication_key = None
    schema_filepath = f"{SCHEMAS_DIR}/ArticleItems.json"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["goodsOwnerId"] = self.config["goods_owner_id"]
        params["articleSystemIdFrom"] = 0
        if next_page_token:
            params["page"] = next_page_token
        return params