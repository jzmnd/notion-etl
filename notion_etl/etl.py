import os
import logging
import sys

import pandas as pd
from dotenv import load_dotenv
from notion_client import Client
from notion_client.helpers import collect_paginated_api
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from notion_etl.configs import EtlJobConfig, add_to_pythonpath, get_transformer_class
from notion_etl.converters import NotionDbConverter
from notion_etl.logging import NotionEtlLogAdapter

LOGGER = logging.getLogger("notion_etl")


class NotionEtl:
    """Main ETL class for running the Notion -> external database job."""

    def __init__(self, config_file_path: str) -> None:
        self.config_file_path = config_file_path
        config_dir = os.path.dirname(self.config_file_path)
        add_to_pythonpath(config_dir)
        self.conf = EtlJobConfig.from_file(self.config_file_path)
        if self.conf.transformer:
            transformer_class = get_transformer_class(self.conf.transformer.class_path)
            self.transformer = transformer_class()
        else:
            self.transformer = None
        self.logger = NotionEtlLogAdapter(LOGGER, {"name": self.conf.name})

    def extract(self) -> pd.DataFrame:
        """Extract all data from the Notion database using the `notion_client` library."""
        self.logger.info("Reading Notion database: %s", self.conf.source.notion_database_id)
        client = Client(auth=os.environ[self.conf.source.notion_token_env], log_level=logging.INFO)
        database_query = collect_paginated_api(
            client.databases.query, database_id=self.conf.source.notion_database_id
        )
        converter = NotionDbConverter()
        data = converter.convert(database_query)
        return pd.DataFrame.from_records(data)

    def load(self, data: pd.DataFrame) -> None:
        """Load data into the external database."""
        engine = create_engine(
            URL.create(
                drivername=self.conf.destination.driver_name,
                username=self.conf.destination.username,
                password=os.environ[self.conf.destination.password_env],
                port=self.conf.destination.port,
                host=self.conf.destination.host,
                database=self.conf.destination.database,
            )
        )
        self.logger.info("Inserting rows into table: %s", self.conf.destination.table_name)
        data.to_sql(
            self.conf.destination.table_name,
            engine,
            index=False,
            if_exists=self.conf.destination.if_exists,
        )
        self.logger.info("Job complete")

    def run(self):
        """Run the job."""
        data = self.extract()
        if self.transformer:
            data = self.transformer.transform(data)
        self.load(data)


def setup_logger() -> None:
    """Setup custom logger for the job."""
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)


if __name__ == "__main__":
    setup_logger()
    load_dotenv()
    config_file_path = sys.argv[1]
    NotionEtl(config_file_path).run()
