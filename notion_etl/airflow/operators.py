from typing import Any, Dict, List, Optional, Union

import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy import create_engine

from notion_etl.airflow.hooks import NotionDbHook
from notion_etl.converters import NotionDbConverter


Json = Union[Dict[str, Any], List[Any], str]


class NotionDbToPostgresOperator(BaseOperator):
    """Copy data from a Notion database to a Postgres table."""

    @apply_defaults
    def __init__(
        self,
        postgres_table: str,
        notion_database_id: str,
        postgres_conn_id: str = "postgres_default",
        postgres_database: Optional[str] = None,
        notion_api_conn_id: Optional[str] = None,
        notion_api_token: Optional[str] = None,
        notion_query_filter: Optional[Json] = None,
        if_exists: str = "replace",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_table = postgres_table
        self.notion_database_id = notion_database_id
        self.postgres_conn_id = postgres_conn_id
        self.postgres_database = postgres_database
        self.notion_api_conn_id = notion_api_conn_id
        self.notion_api_token = notion_api_token
        self.notion_query_filter = notion_query_filter
        self.if_exists = if_exists

    def execute(self, context: Dict) -> None:
        notion_hook = NotionDbHook(token=self.notion_api_token, conn_id=self.notion_api_conn_id)
        postgres_hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.postgres_database
        )
        converter = NotionDbConverter()

        self.log.info("Reading Notion database: %s", self.notion_database_id)
        result = notion_hook.query_db(
            database_id=self.notion_database_id, filter=self.notion_query_filter
        )
        data = converter.convert(result)
        data = pd.DataFrame.from_records(data)

        self.log.info("Inserting rows into Postgres: %s", self.postgres_table)
        engine = create_engine(postgres_hook.sqlalchemy_url)
        data.to_sql(self.postgres_table, engine, index=False, if_exists=self.if_exists)

        self.log.info("Done")
