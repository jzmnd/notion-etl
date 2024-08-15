import datetime as dt
from typing import Any, Dict, List, Tuple, Union
from zoneinfo import ZoneInfo

from dateutil.parser import isoparse

from notion_etl.strenum import StrEnum, auto


Json = Union[Dict[str, Any], List[Any], str]


class NotionDbTypes(StrEnum):
    """Notion database property types.

    See https://developers.notion.com/reference/property-object
    """

    CHECKBOX = auto()
    CREATED_BY = auto()
    CREATED_TIME = auto()
    DATE = auto()
    EMAIL = auto()
    FILES = auto()
    FORMULA = auto()
    LAST_EDITED_BY = auto()
    LAST_EDITED_TIME = auto()
    MULTI_SELECT = auto()
    NUMBER = auto()
    PEOPLE = auto()
    PHONE_NUMBER = auto()
    RELATION = auto()
    RICH_TEXT = auto()
    ROLLUP = auto()
    SELECT = auto()
    STATUS = auto()
    TITLE = auto()
    URL = auto()


def parse_plain_text_str_join(item: Json) -> str:
    return "".join(i["plain_text"] for i in item) or None


def parse_basic_type(item: Json) -> str:
    return item


def parse_nested_type(item: Json) -> str:
    return item[item["type"]]


def parse_datetime(item: Json) -> Union[dt.datetime, Tuple[dt.datetime, dt.datetime]]:
    start = isoparse(item["start"])
    end = isoparse(item["end"]) if "end" in item else None
    tz = ZoneInfo(item["time_zone"]) if "time_zone" in item else None

    if tz:
        start = start.astimezone(tz)
        if end:
            end = end.astimezone(tz)

    return (start, end) if end else start


def parse_id(item: Json) -> str:
    return item["id"]


def parse_name(item: Json) -> str:
    return item["name"]


def parse_id_array(item: Json) -> List[str]:
    return [i["id"] for i in item]


def parse_name_array(item: Json) -> List[str]:
    return [i["name"] for i in item]


class NotionDbConverter:
    """Convert Notion API database json response to a dataframe."""

    CONVERT_FCNS = {
        NotionDbTypes.CHECKBOX: parse_basic_type,
        NotionDbTypes.CREATED_BY: parse_id,
        NotionDbTypes.CREATED_TIME: parse_basic_type,
        NotionDbTypes.DATE: parse_datetime,
        NotionDbTypes.EMAIL: parse_basic_type,
        NotionDbTypes.FILES: parse_name_array,
        NotionDbTypes.FORMULA: parse_nested_type,
        NotionDbTypes.LAST_EDITED_BY: parse_id,
        NotionDbTypes.LAST_EDITED_TIME: parse_basic_type,
        NotionDbTypes.MULTI_SELECT: parse_name_array,
        NotionDbTypes.NUMBER: parse_basic_type,
        NotionDbTypes.PEOPLE: parse_id_array,
        NotionDbTypes.PHONE_NUMBER: parse_basic_type,
        NotionDbTypes.RELATION: parse_id_array,
        NotionDbTypes.RICH_TEXT: parse_plain_text_str_join,
        NotionDbTypes.ROLLUP: parse_nested_type,
        NotionDbTypes.SELECT: parse_name,
        NotionDbTypes.STATUS: parse_name,
        NotionDbTypes.TITLE: parse_plain_text_str_join,
        NotionDbTypes.URL: parse_basic_type,
    }

    def convert(self, json: Json) -> List[Dict[str, Any]]:
        """Convert method.

        See https://developers.notion.com/reference/property-value-object
        """
        data = []

        if isinstance(json, list):
            results = json
        elif isinstance(json, dict):
            results = json["results"]
        else:
            raise TypeError("JSON response must be either a list or dict")

        for result in results:
            properties = result["properties"]

            row = {}
            for header, content in properties.items():
                property_type = content["type"]
                item = content[property_type]
                convert_fcn = self.CONVERT_FCNS[property_type]
                row[header] = convert_fcn(item)

            data.append(row)

        return data
