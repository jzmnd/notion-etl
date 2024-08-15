from __future__ import annotations

import importlib
import os
import sys
from dataclasses import dataclass
from typing import Optional, Type

import yaml
from dacite import from_dict

from notion_etl.transformer import BaseTransformer


def add_to_pythonpath(directory: str):
    """Dynamically add the configurations directory to PYTHONPATH."""
    absolute_path = os.path.abspath(directory)
    if absolute_path not in sys.path:
        sys.path.append(absolute_path)


def get_transformer_class(class_path: str) -> Type[BaseTransformer]:
    """Load the transformer class from a string path."""
    module_name, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    transformer_class = getattr(module, class_name)
    if not issubclass(transformer_class, BaseTransformer):
        raise TypeError("Transformer class must be a subclass of the `BaseTransformer`")
    return transformer_class


@dataclass
class SourceConfig:

    notion_database_id: str
    notion_token_env: str


@dataclass
class TransformerConfig:

    class_path: str


@dataclass
class DestinationConfig:

    table_name: str
    driver_name: str
    database: str
    host: str
    port: int
    username: str
    password_env: str
    if_exists: str


@dataclass
class EtlJobConfig:
    """ETL job configuration class."""

    name: str
    source: SourceConfig
    transformer: Optional[TransformerConfig]
    destination: DestinationConfig

    @classmethod
    def from_file(cls, file_path: str) -> EtlJobConfig:
        with open(file_path, "r") as stream:
            config = yaml.safe_load(stream)
        return from_dict(data_class=cls, data=config)
