import json
import logging
import re
from copy import deepcopy
from datetime import datetime as datetime_
from datetime import timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, cast

from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc
from geojson import GeoJSON
from geojson import dumps as geojson_dumps
from pydantic import root_validator, validator

# Thought of using attrs but we need that validation of pydantic
from pydantic.dataclasses import dataclass

from pystac_user.types import (
    BBox,
    CollectionsLike,
    Datetimes,
    Fields,
    FieldsLike,
    FilterLang,
    FilterLike,
    IdsLike,
    IntersectsLike,
    Limit,
    QueryLike,
    SortByLike,
)
from pystac_user.utils import merge_schemas_dict

logger = logging.getLogger("pystac_user")

DEFAUL_LIMIT = 100
_QUERY_OPERATOR: List[str] = [
    "eq",
    "neq",
    "lt",
    "lte",
    "gt",
    "gte",
    "startsWith",
    "endsWith",
    "contains",
    "in",
]
_OPERATOR_MAP: Dict[str, str] = {
    "==": "eq",
    "!=": "neq",
    "<": "lt",
    "<=": "lte",
    ">": "gt",
    ">=": "gte",
}
DATETIME_REGEX = re.compile(
    r"^(?P<year>\d{4})(-(?P<month>\d{2})(-(?P<day>\d{2})"
    r"(?P<remainder>([Tt])\d{2}:\d{2}:\d{2}(\.\d+)?"
    r"(?P<tz_info>[Zz]|([-+])(\d{2}):(\d{2}))?)?)?)?$"
)


@dataclass
class Query:
    """Query parameters for a STAC. Can be used for STAC API and static STACs

    The query structure is defined here:
        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/query

    The schema looks like this:
        {
            <property>: {
                {<operator1>}: <value1>
                {<operator2>}: <value2>
        }
    Examples:
        >>> query = Query(property="datetime", operator=[("eq", "2020-01-01")])
        >>> assert query.dict() == {'datetime': {'eq': '2020-01-01'}}
        >>> query = Query(property="datetime",
        ...         operator=[(">", "2020-01-01"), ("<", "2020-01-02")])
        >>> assert query.dict()
        ... == {'datetime': {'gt': '2020-01-01', 'lt': '2020-01-02'}}
    Arguments:
        property (str) -- Property to query
        operator (List[Tuple[str, Any]]) -- Operator and value to query.
            Operator can be one of the following: `QUERY_OPERATOR`.
            But also can be one of the following: `==`, `!=`, `<`, `<=`, `>`, `>=`.
    """

    property: str
    operator: List[Tuple[str, Any]]

    @validator("operator", each_item=True)
    def check_operator(cls, v: Tuple[str, Any]) -> Tuple[str, Any]:
        """Check if operator is valid with `_QUERY_OPERATOR`.

        Raises:
            ValueError: If the operator is not valid.
        """
        op, value = v
        # If the operator is in the map, replace it
        if op in _OPERATOR_MAP:
            op = _OPERATOR_MAP[op]

        # Check if it is in the valid operators. If not raise an error
        if op not in _QUERY_OPERATOR:
            raise ValueError(
                f"""Operator {op} is not valid it should be one of
                    {_QUERY_OPERATOR} or {_OPERATOR_MAP.keys()}"""
            )
        return (op, value)

    # def __post_init__(self):
    #     # Check if operator is valid
    #     for i in range(len(self.operator)):
    #         op, value = self.operator[i]
    #         if op in _OPERATOR_MAP:
    #             self.operator[i] = (_OPERATOR_MAP[op], value)
    #         elif op not in _QUERY_OPERATOR:
    #             raise ValueError(f"Operator {op} is not valid")

    def dict(self) -> Dict[str, Any]:
        """Return a dict representation of the query

        Returns:
            Dict[str, Any] -- A dict representation of the query.
        """
        out: Dict[str, Any] = {self.property: {}}
        for op, value in self.operator:
            out[self.property][op] = value

        return out


@dataclass
class Filter:
    """Filter parameters for a STAC. Can be used for STAC API and static STACs.

    The filter structure is defined here:
        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/filter

    The schema looks like this:
        {
            "op": "<operator>",
            "args": [
                {
                    "op": "<operator1>",
                    "args": [ {<property> : <value>}, value1, ...]
                },
                {
                    "op": "<operator2>",
                    "args": [ {<property1> : <value2>}]
                }
            ]
        }

    Examples:
        >>> filter = Filter(op="and",
        ...                 args=[{"op": "=", "args": ["datetime", "2020-01-01"]},
        >>> assert filter.dict()
        ... == {"op": "=", "args": [{"op": "=", "args": ["datetime", "2020-01-01"]},
        >>> filter = Filter(op="s_intersects",
                            args=[
                                { "property": "geometry" },
                                { "type": "Polygon", "coordinates":
                                    [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}])
        >>> assert filter.dict()
        ... == {"op": "s_intersects", "args": [{"property": "geometry"},
        ... {"type": "Polygon", "coordinates":
        ...         [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}]}

    Arguments:
        op (str) -- Operator to use for the filter.
        args (List[Dict[str, Any]]) -- Arguments for the filter.
    """

    op: str
    args: List[Dict[str, Any]]

    def dict(self) -> Dict[str, Any]:
        """Return a dict representation of the filter"""
        out = {"op": self.op, "args": self.args}
        return out


@dataclass
class SortBy:
    """Sort parameters for a STAC. Can be used for STAC API and static STACs.

    The sort structure is defined here:
        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/sort

    The schema looks like this:
        {
            "field": "<property_name>",
            "direction": "<direction>"
        }

    Examples:
        >>> sortby = SortBy(field="datetime", direction="asc")
        >>> assert sortby.dict() == {"field": "datetime", "direction": "asc"}
        >>> assert str(sortby) == "+datetime"
        >>> sortby = SortBy(field="datetime", direction="desc")
        >>> assert sortby.dict() == {"field": "datetime", "direction": "desc"}
        >>> assert str(sortby) == "-datetime"
        >>> sortbys = SortBy.from_str("+datetime,-cloud_cover")
        >>> assert sortbys[0].dict() == {"field": "datetime", "direction": "asc"}
        >>> assert sortbys[1].dict() == {"field": "cloud_cover", "direction": "desc"}
        >>> sortbys = SortBy.from_str("datetime,cloud_cover")
        >>> assert sortbys[0].dict() == {"field": "datetime", "direction": "asc"}
        >>> assert sortbys[1].dict() == {"field": "cloud_cover", "direction": "asc"}

    Arguments:
        field (str) -- Field to sort by.
        direction (Literal["asc", "desc"]) -- Direction to sort by.
                                                Can be "asc" or "desc".
    """

    field: str
    direction: Literal["asc", "desc"]

    def dict(self) -> Dict[str, Any]:
        """Return a dict representation of the sort"""
        out = {"field": self.field, "direction": cast(str, self.direction)}
        return out

    def __str__(self) -> str:
        """Return a string representation of the sort"""
        sgn = "+" if self.direction == "asc" else "-"
        out = f"{sgn}{self.field}"
        return out

    @classmethod
    def from_str(cls, part: str) -> List["SortBy"]:
        """Create a list of SortBy from a string.

        Args:
            part (str): string to parse.
                Should be a comma separated list of fields with a + or - prefix.

        Returns:
            List[SortBy]: list of SortBy
        """
        sortby_list: List[SortBy] = []
        # Split on comma
        for p in part.split(","):
            # Check if the field is prefixed with + or -
            if p.startswith("-"):
                sortby_list.append(cls(field=p[1:], direction="desc"))
            elif p.startswith("+"):
                sortby_list.append(cls(field=p[1:], direction="asc"))
            # If not, assume it's ascending
            else:
                sortby_list.append(cls(field=p, direction="asc"))
        return sortby_list


@dataclass
class Field:
    """Fields parameters for a STAC. Can be used for STAC API and static STACs.

    The fields structure is defined here:
        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/fields

    The schema looks like this:
        {
            "include": ["<property1>", "<property2>", ...],
            "exclude": ["<property1>", "<property2>", ...]
        }

    Examples:
        >>> field = Field(field_type="include", fields=["datetime", "cloud_cover"])
        >>> assert field.dict() == {"include": ["datetime", "cloud_cover"]}
        >>> assert str(field) == "+datetime,+cloud_cover"
        >>> field = Field(field_type="exclude", fields=["datetime", "cloud_cover"])
        >>> assert field.dict() == {"exclude": ["datetime", "cloud_cover"]}
        >>> assert str(field) == "-datetime,-cloud_cover"
        >>> fields = Field.from_str("+datetime,-cloud_cover")
        >>> assert fields[0].dict() == {"include": ["datetime"]}
        >>> assert fields[1].dict() == {"exclude": ["cloud_cover"]}
        >>> fields = Field.from_str("datetime,cloud_cover")
        >>> assert fields[0].dict() == {"include": ["datetime", "cloud_cover"]}
        >>> assert fields[1] is None
    """

    field_type: Literal["include", "exclude"]
    fields: List[str]

    def dict(self) -> Dict[str, List[str]]:
        """Return a dict representation of the fields"""
        out = {cast(str, self.field_type): self.fields}
        return out

    def __str__(self) -> str:
        """Return a string representation of the fields"""
        sgn = "+" if self.field_type == "include" else "-"
        out = ",".join([f"{sgn}{f}" for f in self.fields])
        return out

    @classmethod
    def from_str(cls, fields: str) -> Tuple[Optional["Field"], Optional["Field"]]:
        """Create a tuple of at least two Fields from a string.

        Args:
            fields (str): string to parse.

        Raises:
            ValueError: If no fields are provided.

        Returns:
            Tuple[Optional[Field], Optional[Field]]: Tuple of at least one Field.
                If only one field is provided, the second element will be None.
                Fields are separated to `include` and `exclude` fields.
                Tuple is ordered as (include, exclude).
        """
        includes: List[str] = []
        excludes: List[str] = []
        # Split on comma
        for field in fields.split(","):
            # Check if the field is prefixed with + or -
            if field.startswith("-"):
                excludes.append(field[1:])
            elif field.startswith("+"):
                includes.append(field[1:])
            # If not, assume it's included
            else:
                includes.append(field)

        # Create the fields
        field_includes = (
            cls(field_type="include", fields=includes) if len(includes) > 0 else None
        )
        field_exclude = (
            cls(field_type="exclude", fields=excludes) if len(excludes) > 0 else None
        )

        # Check if we have at least one field
        if field_includes is None and field_exclude is None:
            raise ValueError("No fields provided")

        return field_includes, field_exclude


@dataclass
class Search:
    """Search parameters for a STAC.
    Can be used for STAC API endpoint as described in the `STAC API - Item Search spec
    <https://github.com/radiantearth/stac-api-spec/tree/master/item-search>`__,
    and can be used for static STACs search.

    No request is sent to the API until a method is called to iterate
    through the resulting STAC Items, either :meth:`ItemSearch.item_collections`,
    :meth:`ItemSearch.items`, or :meth:`ItemSearch.items_as_dicts`.

    All parameters correspond to query parameters
    described in the `STAC API - Item Search: Query Parameters Table
    <https://github.com/radiantearth/stac-api-spec/tree/master/item-search#query-parameter-table>`__
    docs. Please refer
    to those docs for details on how these parameters filter search results.

    Raises:
        ValueError: If a datetime component is invalid.

    Attributes:
        search_type (Literal["api", "static"]): Type of search.
        bbox (BBox | None):
            Only return items that intersect with the provided bounding box.
        intersects (IntersectsLike | None):
            Only return items that intersect with the provided GeoJSON geometry.
            Can be a GeoJSON-like dict or a GeoJSON object.
        datetime (Datetimes | None):
            Either a single datetime or datetime range used to filter results.
            You may express a single datetime using a :class:`datetime.datetime`
            instance, a `RFC 3339-compliant <https://tools.ietf.org/html/rfc3339>`__
            timestamp, or a simple date string (see below). Instances of
            :class:`datetime.datetime` may be either
            timezone aware or unaware. Timezone aware instances will be converted to
            a UTC timestamp before being used.
            Timezone unaware instances are assumed to represent UTC
            timestamps. You may represent a
            datetime range using a ``"/"`` separated string as described in the spec,
            or a list, tuple of 2 timestamps or datetime instances.
            If using a simple date string, the datetime can be specified in
            ``YYYY-mm-dd`` format, optionally truncating
            to ``YYYY-mm`` or just ``YYYY``. Simple date strings will be expanded to
            include the entire time period, for example:

            - ``2017`` expands to ``2017-01-01T00:00:00Z/2017-12-31T23:59:59Z``
            - ``2017-06`` expands to ``2017-06-01T00:00:00Z/2017-06-30T23:59:59Z``
            - ``2017-06-10`` expands to ``2017-06-10T00:00:00Z/2017-06-10T23:59:59Z``

            If used in a range, the end of the range expands to the end of that
            day/month/year, for example:

            - ``2017/2018`` expands to
              ``2017-01-01T00:00:00Z/2018-12-31T23:59:59Z``
            - ``2017-06/2017-07`` expands to
              ``2017-06-01T00:00:00Z/2017-07-31T23:59:59Z``
            - ``2017-06-10/2017-06-11`` expands to
              ``2017-06-10T00:00:00Z/2017-06-11T23:59:59Z``
        ids (IdsLike | None): Only return items that match the provided item IDs.
        collections (CollectionsLike | None):
            Only return items that are part of the provided collections ids.
        query (QueryLike | None):
            Only return items that match the provided key-value pairs,
            specified in `Query` dataclass. Accepts list of Query objects.
        filter (FilterLike | None):
            Accepts a filter expression as a string or a Filter object.
        filter_lang (FilterLang | None):
            Accepts a filter language `cql2-json` or `cql2-text`. If not provided,
            the filter language is based on the `filter` attribute type.
        sort_by (SortByLike | None):
            Sorts the results by the provided fields. Accepts list of SortBy objects
            or string that can be converted to Sortby objects.
        fields (FieldLike | None):
            Only return the specified fields in the response. Accepts Field objects or
            string that can be converted to Field objects. If provided only one field it
            will be to Field tuple with one None value.
        limit (Limit): Limit the number of items returned. Defaults to `DEFAUL_LIMIT`.
    """

    search_type: Literal["api", "static"]
    bbox: Optional[BBox]
    intersects: Optional[IntersectsLike]
    datetime: Optional[Tuple[datetime_, Optional[datetime_]]]
    ids: Optional[IdsLike]
    collections: Optional[CollectionsLike]
    query: Optional[QueryLike]
    filter: Optional[FilterLike]
    filter_lang: Optional[FilterLang]
    sort_by: Optional[SortByLike]
    fields: Optional[FieldsLike]
    limit: Limit = DEFAUL_LIMIT

    @staticmethod
    def _to_utc_isoformat(dt: datetime_) -> str:
        """Converts a datetime to UTC ISO format.

        Args:
            dt (datetime_): `datetime` instance to convert.

        Returns:
            str: UTC ISO format string.
        """
        dt = dt.astimezone(timezone.utc)
        dt = dt.replace(tzinfo=None)
        return f'{dt.isoformat("T")}Z'

    def _to_datetime_range(
        self, component: str
    ) -> Tuple[datetime_, Optional[datetime_]]:
        """Converts a string like datetime component to a datetime range.

        Args:
            component (str): String like datetime component.

        Raises:
            ValueError: If the component is invalid.

        Returns:
            Tuple[datetime_, Optional[datetime_]]: A tuple of start and end datetimes.
                end datetime can be None depending on the component. If the component
                is full datetime format than end datetime will be None.
        """
        match = DATETIME_REGEX.match(component)
        if not match:
            raise ValueError(f"invalid datetime component: {component}")
        elif match.group("remainder"):
            if match.group("tz_info"):
                return datetime_.fromisoformat(component), None
            else:
                return datetime_.fromisoformat(f"{component}Z"), None
        else:
            year = int(match.group("year"))
            optional_month = match.group("month")
            optional_day = match.group("day")

        if optional_day is not None:
            start = datetime_(
                year,
                int(optional_month),
                int(optional_day),
                0,
                0,
                0,
                tzinfo=tzutc(),
            )
            end = start + relativedelta(days=1, seconds=-1)
        elif optional_month is not None:
            start = datetime_(year, int(optional_month), 1, 0, 0, 0, tzinfo=tzutc())
            end = start + relativedelta(months=1, seconds=-1)
        else:
            start = datetime_(year, 1, 1, 0, 0, 0, tzinfo=tzutc())
            end = start + relativedelta(years=1, seconds=-1)
        return start, end

    @root_validator
    def _validate_positioning(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Validates the intersects and bbox attributes.
        If both are provided, bbox is ignored. If intersects is provided, it is
        converted to GeoJSON.

        Args:
            v (Dict): The dictionary of attributes.

        Returns:
            Dict: The dictionary of attributes.
        """
        # Check if bbox and intersects are mutually exclusive
        intersects, bbox = v.get("intersects"), v.get("bbox")
        if intersects is not None and bbox is not None:
            logger.warning("bbox and intersects are mutually exclusive, bbox ignored")
            v["bbox"] = None

        # Convert intersects to GeoJSON
        if intersects is not None:
            if not isinstance(intersects, GeoJSON):
                v["intersects"] = GeoJSON.to_instance(intersects)

        return v

    @validator("datetime", pre=True, always=True)
    def _validate_datetime(
        cls, datetime: Datetimes
    ) -> Optional[Tuple[datetime_, Optional[datetime_]]]:
        """Validates the datetime attribute. If provided, it is converted to
        converted to start,end datetime range. If the datetime is whole datetime
        the end datetime will be None.

        Raises:
            ValueError: If the datetime range is invalid.
            ValueError: If the datetime initial type is invalid.

        Args:
            v (Datetimes): The datetime or string date with range option.

        Returns:
            Optional[Tuple[datetime_, Optional[datetime_]]]::
                                            The start and end datetime range.
        """
        new_datetime: Optional[Tuple[datetime_, Optional[datetime_]]] = None
        if datetime is not None:
            if isinstance(datetime, str):
                # Convert date range to tuple of datetimes
                if "/" in datetime:
                    datetimes = tuple(d for d in datetime.split("/"))
                    if len(datetime) != 2:
                        raise ValueError(
                            f"""datetime range must be max 2 values
                                and is {len(datetime)}"""
                        )
                    start, _ = cls._to_datetime_range(datetimes[0])
                    backup_end, end = cls._to_datetime_range(datetimes[1])
                    new_datetime = (start, end or backup_end)
                else:  # Single date string
                    new_datetime = cls._to_datetime_range(datetime)
            # Convert datetime to tuple of datetimes with None end
            elif isinstance(datetime, datetime_):
                new_datetime = (datetime, None)
            else:
                raise ValueError(f"invalid datetime type: {type(datetime)}")

        return new_datetime

    @validator("query")
    def _convert_query(cls, query: Optional[QueryLike]) -> Optional[List[Query]]:
        """Converts the query attribute to a Query object.

        Args:
            query (Optional[QueryLike]): The query string or list of query objects.

        Returns:
             Optional[List[Query]]: The list of Query objects.
        """

        new_query: Optional[List[Query]] = None
        if query is not None:
            new_query = []
            # Convert query List of dict to List of Query objects
            for q in query:
                if isinstance(q, Query):
                    new_query.append(q)
                else:
                    new_query.append(Query(property=q[0], operator=q[1]))
        return new_query

    @validator("filter")
    def _convert_filter(cls, filter: FilterLike) -> Union[Optional[Filter], str]:
        """Converts the filter attribute to a Filter object.

        Exceptions:
            ValueError: If the filter type is invalid.

        Args:
            filter (FilterLike): The filter object dict or string.

        Returns:
            Union[Optional[Filter], str]: The Filter object or None or string.
        """
        new_filter: Union[Optional[Filter], str] = None
        if filter is not None:
            # If filter is not a string it is a dict
            # Convert filter dict to Filter object
            if not isinstance(filter, str):
                if not isinstance(filter, tuple):
                    raise ValueError(
                        f"""invalid filter type if not string: {type(filter)}
                        should be tuple"""
                    )
                new_filter = Filter(op=filter[0], args=filter[1])
            else:
                new_filter = filter
        return new_filter

    @root_validator
    def _validate_filter_lang(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Validates the filter_lang attribute.
        If not provided, it is set depending on `filter` type.

        Args:
            v (Dict): The dictionary of attributes.

        Returns:
            Dict: The dictionary of attributes.
        """
        if v.get("filter_lang") is None:
            # If filter is string it is cql2-text
            if isinstance(v.get("filter"), str):
                v["filter_lang"] = "cql2-text"
            else:  # Otherwise it is cql2-json
                v["filter_lang"] = "cql2-json"
        return v

    @validator("sort_by")
    def _convert_sortby(cls, sortby: SortByLike) -> Optional[List[SortBy]]:
        """Converts the sortby attribute to a SortBy object.
        If string is provided, it is converted to a list of SortBy object.
        Else if a list is provided, it is converted to a list of SortBy objects.

        Args:
            sortby (SortByLike): list of SortBy objects or string.
        Returns:
            Optional[List[SortBy]]: The list of SortBy objects.
        """
        new_sortby: Optional[List[SortBy]] = None
        if sortby is not None:
            # Convert sortby string to list of SortBy objects
            if isinstance(sortby, str):
                new_sortby = SortBy.from_string(sortby)  # type: ignore
            else:  # Convert sortby list of dict to list of SortBy objects
                new_sortby = []
                sortby = cast(List, sortby)
                for i in range(len(sortby)):
                    if isinstance(sortby[i], SortBy):
                        sortby_item = cast(SortBy, sortby[i])
                        new_sortby.append(sortby_item)
                    else:
                        for field_sortby in sortby[i][1]:
                            new_sortby.append(
                                SortBy(field=field_sortby, direction=sortby[i][0])
                            )
        return new_sortby

    @validator("fields")
    def _convert_fields(
        cls, fields: FieldsLike
    ) -> Optional[Tuple[Optional[Field], Optional[Field]]]:
        """Converts the fields attribute to a Fields object.
        If string is provided, it is converted to a tuple of Fields object.
        Else if a list is provided, it is converted to a list of Fields objects.
        Else it is a one Filed object and we create a tuple with None.

        Args:
            fields (FieldsLike): list of Fields objects or string.
        Returns:
            Optional[Tuple[Optional[Field], Optional[Field]]]:
                The tuple of Fields objects.
        """
        new_fields: Optional[Tuple[Optional[Field], Optional[Field]]] = None
        if fields is not None:
            # Convert fields string to tuple of Fields objects
            if isinstance(fields, str):
                new_fields = Field.from_string(fields)  # type: ignore
            # Convert fields list of dict to list of Fields objects
            elif isinstance(fields, tuple):
                fields = cast(Tuple[Fields, Fields], fields)
                field_first_data, field_second_data = fields
                field_first = Field(
                    field_type=field_first_data[0], fields=field_first_data[1]
                )
                field_second = Field(
                    field_type=field_second_data[0], fields=field_second_data[1]
                )
                if field_first.field_type == "include":
                    new_fields = (field_first, field_second)
                else:
                    new_fields = (field_second, field_first)
            else:  # Convert filed dict to Field object
                fields = cast(Fields, fields)
                field = Field(field_type=fields[0], fields=fields[1])
                if field.field_type == "include":
                    new_fields = (field, None)
                else:
                    new_fields = (None, field)
        return new_fields

    # def __post_init__(self):
    #     # Check if bbox and intersects are mutually exclusive
    #     if self.bbox is not None and self.intersects is not None:
    #         logger.warning(
    #             "bbox and intersects are mutually exclusive, bbox will be ignored"
    #         )
    #         self.bbox = None

    #     # Convert bbox to GeoJSON
    #     if self.intersects is not None:
    #         if not isinstance(self.intersects, GeoJSON):
    #             self.intersects = GeoJSON.to_instance(self.intersects)

    #     # Convert datetime to datetime object TODO
    #     if self.datetime is not None:
    #         if isinstance(self.datetime, str):
    #             if "/" in self.datetime:
    #                 datetimes = tuple(d for d in self.datetime.split("/"))
    #                 if len(self.datetime) != 2:
    #                     raise ValueError(
    #                         f"""datetime range must be max 2 values
    #                             and is {len(self.datetime)}"
    #                     )
    #                 start, _ = self._to_datetime_range(datetimes[0])
    #                 backup_end, end = self._to_datetime_range(datetimes[1])
    #                 self.datetime = (start, end or backup_end)
    #             else:
    #                 self.datetime = self._to_datetime_range(self.datetime)
    #         elif isinstance(self.datetime, datetime_):
    #             self.datetime = (self.datetime,)

    #     # Validate query
    #     if self.query is not None:
    #         for i in range(len(self.query)):
    #             if not isinstance(self.query[i], Query):
    #                 self.query[i] = Query(**self.query[i])

    #     # Validate filter_lang
    #     if self.filter is not None and self.filter_lang is not None:
    #         if isinstance(self.filter, str):
    #             self.filter_lang = "cql2-text"
    #         elif isinstance(self.filter, dict):
    #             self.filter_lang = "cql2-json"

    #     # Validate filter
    #     if self.filter is not None:
    #         if not isinstance(self.filter, str):
    #             self.filter = Filter(**self.filter)

    #     # Validate sort_by
    #     if self.sort_by is not None:
    #         if isinstance(self.sort_by, str):
    #             self.sort_by = SortBy.from_str(self.sort_by)
    #         else:
    #             for i in range(len(self.sort_by)):
    #                 if not isinstance(self.sort_by[i], SortBy):
    #                     self.sort_by[i] = SortBy(**self.sort_by[i])

    #     # Validate fields
    #     if self.fields is not None:
    #         if isinstance(self.fields, str):
    #             self.fields = Field.from_str(self.fields)
    #         if isinstance(self.fields, tuple):
    #             # One field must be included the other excluded
    #             field_first, field_second = self.fields
    #             field_first = Field(**field_first)
    #             field_second = Field(**field_second)
    #             self.fields = (field_first, field_second)
    #         else:
    #             self.fields = (Field(**self.fields),)

    def dict(self) -> Dict[str, Any]:
        """Return a dict representation of the cls

        Returns:
            Dict: The dict representation of the search
        """
        out: Dict[str, Any] = {}
        if self.bbox is not None:
            out["bbox"] = self.bbox
        if self.intersects is not None:
            out["intersects"] = json.loads(geojson_dumps(self.intersects))
        if self.datetime is not None:
            if self.datetime[0] is None:
                out["datetime"] = self._to_utc_isoformat(self.datetime[1])
            elif self.datetime[1] is None:
                out["datetime"] = self._to_utc_isoformat(self.datetime[0])
            else:
                out["datetime"] = (
                    f"{self._to_utc_isoformat(self.datetime[0])}"
                    "/"
                    f"{self._to_utc_isoformat(self.datetime[1])}"
                )

        if self.ids is not None:
            out["ids"] = self.ids
        if self.collections is not None:
            out["collections"] = self.collections
        if self.query is not None:
            out["query"] = merge_schemas_dict(self.query)
        if self.filter is not None:
            if isinstance(self.filter, str):
                out["filter"] = self.filter
            elif isinstance(self.filter, Filter):
                filter = cast(Filter, self.filter)
                out["filter"] = filter.dict()
        if self.filter_lang is not None:
            out["filter-lang"] = self.filter_lang
        if self.sort_by is not None:
            sort_by = cast(List[SortBy], self.sort_by)
            out["sortby"] = [s.dict() for s in sort_by]
        if self.fields is not None:
            if self.fields[0] is not None and self.fields[1] is not None:
                fields = cast(Tuple[Field, Field], self.fields)
                out["fields"] = merge_schemas_dict([fields[0].dict(), fields[1].dict()])
            elif self.fields[0] is not None:
                out["fields"] = self.fields[0].dict()
            else:
                out["fields"] = self.fields[1].dict()
        if self.limit is not None:
            out["limit"] = self.limit
        return out

    def get_request(self) -> Dict[str, Any]:
        """Return a dict representation of the search for GET requests.

        Returns:
            Dict: The dict representation of the search
        """
        search_params = deepcopy(self.dict())
        if "bbox" in search_params:
            search_params["bbox"] = ",".join(map(str, search_params["bbox"]))
        if "ids" in search_params:
            search_params["ids"] = ",".join(search_params["ids"])
        if "collections" in search_params:
            search_params["collections"] = ",".join(search_params["collections"])
        if "intersects" in search_params:
            search_params["intersects"] = geojson_dumps(self.intersects)
        if "sortby" in search_params and self.sort_by is not None:
            search_params["sortby"] = ",".join(str(sb) for sb in self.sort_by)
        if "fields" in search_params and self.fields is not None:
            if self.fields[0] is not None and self.fields[1] is not None:
                fields = cast(Tuple[Field, Field], self.fields)
                search_params["fields"] = ",".join(str(f) for f in fields)
            elif self.fields[0] is not None:
                search_params["fields"] = str(self.fields[0])
            else:
                search_params["fields"] = str(self.fields[1])

        return search_params

    def post_request(self) -> Dict[str, Any]:
        """Return a dict representation of the search for POST requests.

        Returns:
            Dict: The dict representation of the search
        """
        return deepcopy(self.dict())
