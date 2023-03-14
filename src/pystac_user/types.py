from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Tuple, Type, Union

from geojson import GeoJSON

# Query Parameter Table

# Limit of objects
Limit = Optional[int]
# Box 2D or 3D
BBox = Union[
    Tuple[float, float, float, float],  # 2D bbox
    Tuple[float, float, float, float, float, float],  # 3D bbox
]
# Datetime can be a string, datetime object or two datetime objects
# for a range. If string is with `/` it is a range
Datetimes = Union[str, datetime, Tuple[datetime, datetime]]
# Geometry object that geojson can parse
Intersects = Type[GeoJSON]
IntersectsLike = Union[Dict[str, Any], Intersects]
# List of string ids of items
IdsLike = List[str]
# List of string ids of collections
CollectionsLike = List[str]
# Query parameters
QueryLike = List[Tuple[str, List[Tuple[str, Any]]]]
# Filter parameters
Filters = Tuple[str, List[Dict[str, Any]]]
FilterLike = Union[str, Filters]
# Filter lang
FilterLang = Literal["cql2-json", "cql2-text"]
# Sort parameters by field and direction
SortBys = List[Tuple[Literal["asc", "desc"], List[str]]]
SortByLike = Union[str, SortBys]
# Fields
Fields = Tuple[Literal["include", "exclude"], List[str]]
FieldsLike = Union[str, Fields, Tuple[Fields, Fields]]