from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Tuple, Type, Union

# Query Parameter Table
from geojson import GeoJSON


class CQL2(Enum):
    """CQL2 enum for filter_lang"""

    JSON = "cql2-json"
    TEXT = "cql2-text"


# Limit of objects
Limit = Optional[int]
# Box 2D or 3D
BBox = Union[
    Tuple[float, float, float, float],  # 2D bbox
    Tuple[float, float, float, float, float, float],  # 3D bbox
]
# Datetime can be a string, datetime object or two datetime objects
# for a range. If string is with `/` it is a range
Datetimes = Union[str, datetime, Tuple[datetime, Optional[datetime]]]
# Geometry object that geojson can parse
Intersects = Type["GeoJSON"]
IntersectsLike = Union[Dict[str, Any], Intersects]
# List of string ids of items
Ids = List[str]
IdsLike = Union[Ids, str]
# List of string ids of collections
Collections = List[str]
CollectionsLike = Union[Collections, str]
# Query parameters
Queries = Tuple[str, List[Tuple[str, Any]]]
QueryLike = List[Queries]
# Filter parameters
Filters = Tuple[str, List[Dict[str, Any]]]
FilterLike = Union[str, Filters]
# Filter lang
FilterLang = Literal[CQL2.TEXT, CQL2.JSON]
# Sort parameters by field and direction
SortBys = List[Tuple[Literal["asc", "desc"], List[str]]]
SortByLike = Union[str, SortBys]
# Fields
Fields = Tuple[Literal["include", "exclude"], List[str]]
FieldsLike = Union[str, Fields, Tuple[Fields, Fields]]
