"""REST client handling, including ongoingStream base class."""

import requests
import backoff
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.authenticators import BasicAuthenticator

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from base64 import b64encode
from pendulum import parse
from typing import Any, Callable, Dict, Iterable, Optional
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from simplejson.scanner import JSONDecodeError


class ongoingStream(RESTStream):
    """ongoing stream class."""

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$.next_page"
    stream_params = None
    paginate = None
    timeout = 2400
    windows_days = 30

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        warehouse = self.config["warehouse_name"]
        api_server = self.config.get("api_server", "api")
        return f"https://{api_server}.ongoingsystems.se/{warehouse}/api/v1/"

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object."""
        return BasicAuthenticator.create_for_stream(
            self,
            username=self.config.get("username"),
            password=self.config.get("password"),
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.paginate:
            if not previous_token:
                start_date = self.start_date + timedelta(self.windows_days)
                end_date = start_date + timedelta(self.windows_days)
                return {
                    self.paginate["start"]: start_date,
                    self.paginate["end"]: end_date
                }
            else:
                next_page_token = previous_token.copy()
                start = next_page_token[self.paginate["end"]]
                if start.replace(tzinfo=None) > datetime.utcnow():
                    return None
                next_page_token[self.paginate["start"]] = start
                next_page_token[self.paginate["end"]] = start + timedelta(self.windows_days)
                return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["goodsOwnerId"] = self.config["goods_owner_id"]
        if self.stream_params:
            params.update(self.stream_params)
        if self.paginate:
            if next_page_token:
                params.update(next_page_token)
            else:
                start_date = self.get_starting_timestamp(context)
                config_start_date = parse(self.config.get("start_date"))
                self.start_date = (start_date or config_start_date)
                start_date = self.start_date
                end_date = start_date + timedelta(self.windows_days)
                params[self.paginate["start"]] = start_date
                params[self.paginate["end"]] = end_date
            params[self.paginate["start"]] = params[self.paginate["start"]].isoformat()
            params[self.paginate["end"]] = params[self.paginate["end"]].isoformat()
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def request_decorator(self, func: Callable) -> Callable:
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                JSONDecodeError
            ),
            max_tries=5,
            factor=2,
        )(func)
        return decorator

    def validate_response(self, response):
        super().validate_response(response)
        try:
            _ = response.json() # noqa
        except JSONDecodeError:
            raise RetriableAPIError(f"JSONDecodeError on {response.url}")
