# Notion ETL

Copy Notion databases to an alternative database such as PostgreSQL or MySQL for backup or structured querying.

Provides a general purpose ETL class that reads a given Notion database using the [Notion API](https://developers.notion.com),
converts the response to a `pandas` Dataframe, and inserts the data into another database using SQLAlchemy.

Note that this library is designed for relatively small datasets, which is typical of the way Notion is used.

## Configuration

Job configurations are written in YAML (see `examples`) and secrets stored in a `.env` file.
Optionally a python file can be included with a subclass of `notion_etl.transformer.BaseTransformer` to
perform simple data transformations on the `pandas` Dataframe before writing to the database.

## Basic usage

The library can be used in it's simplest form from the command line by e.g.:

```bash
python -m notion_etl.etl ./examples/example1.yaml
```

Alternatively the `notion_etl.etl.NotionEtl` class can be used in another script e.g.:

```python
from notion_etl.etl import NotionEtl

job = NotionEtl("./examples/example1.yaml")
job.run()
```

## Airflow usage

An Airflow hook for making Notion database connections is provided in `notion_etl.airflow.hooks.NotionDbHook`.

An example Airflow operator that copies data from Notion to a PostgreSQL database is provided in `notion_etl.airflow.operators.NotionDbToPostgresOperator`.
