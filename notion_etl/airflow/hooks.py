import logging
from typing import Any, Dict, List, Optional, Union

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from notion_client import Client
from notion_client.errors import (
    APIResponseError,
    HTTPResponseError,
    RequestTimeoutError,
)
from notion_client.helpers import collect_paginated_api


Json = Union[Dict[str, Any], List[Any], str]


class NotionDbHook(BaseHook):
    """Airflow hook to make connections to a Notion database using the `notion_client` library."""

    def __init__(self, token: Optional[str] = None, conn_id: Optional[str] = None) -> None:
        self.token = self._get_token(token, conn_id)
        self.conn = None

    def _get_token(self, token: Optional[str], conn_id: Optional[str]) -> str:
        """Return the API token either if provided directly or via a Airflow connection."""
        if token is not None:
            return token
        if conn_id is not None:
            conn = self.get_connection(conn_id)
            try:
                return conn.password
            except AttributeError as err:
                raise AirflowException("Missing token (password) in Notion Connection") from err
        raise AirflowException("No valid Notion API token or Connection ID")

    def get_conn(self) -> Client:
        """Return the Notion client object."""
        if self.conn is None:
            self.conn = Client(auth=self.token, logger=self.log, log_level=logging.INFO)
        return self.conn

    def close_conn(self) -> None:
        """Close the Notion client."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def create_db(self, parent_id: str, db_title: str, properties: Json) -> Json:
        """Create a database in Notion. Wraps `databases.create`."""
        conn = self.get_conn()
        try:
            return conn.databases.create(
                parent={"type": "page_id", "page_id": parent_id},
                title=[{"type": "text", "text": {"content": db_title}}],
                properties=properties,
            )
        except (APIResponseError, HTTPResponseError, RequestTimeoutError) as err:
            raise AirflowException("Notion create database failed") from err

    def query_db(self, database_id: str, filter: Optional[Json] = None) -> Json:
        """Query a database in Notion. Wraps `databases.query`."""
        conn = self.get_conn()
        try:
            if filter:
                result = collect_paginated_api(
                    conn.databases.query, database_id=database_id, filter=filter
                )
            else:
                result = collect_paginated_api(conn.databases.query, database_id=database_id)
            return result
        except (APIResponseError, HTTPResponseError, RequestTimeoutError) as err:
            raise AirflowException("Notion query database failed") from err

    def retrieve_db(self, database_id: str) -> Json:
        """Retrieve properties for a database in Notion. Wraps `databases.retrieve`."""
        conn = self.get_conn()
        try:
            return conn.databases.retrieve(database_id=database_id)
        except (APIResponseError, HTTPResponseError, RequestTimeoutError) as err:
            raise AirflowException("Notion retrieve database failed") from err
