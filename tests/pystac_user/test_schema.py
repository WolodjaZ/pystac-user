from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone

import pytest
from geojson import GeoJSON

from pystac_user.exceptions import EmptyAttributeError
from pystac_user.schema import (
    _OPERATOR_MAP,
    _QUERY_OPERATOR,
    DEFAUL_LIMIT,
    Field,
    Filter,
    Query,
    Search,
    SortBy,
)


class TestQuery:
    """
    The schema looks like this:
        {
            <property>: {
                {<operator1>}: <value1>
                {<operator2>}: <value2>
        }
    Need to have versions:
        - dict
    """

    def test_single_query(self):
        query = Query(property="bbox", operator=[("eq", 2)])
        assert query is not None
        assert query.property == "bbox"
        assert query.operator == [("eq", 2)]

    def test_multiple_query(self):
        operator = _QUERY_OPERATOR + list(_OPERATOR_MAP.keys())
        operations = [(op, 3) for op in operator]
        excepted_operations = [
            (op, 3) for op in _QUERY_OPERATOR + list(_OPERATOR_MAP.values())
        ]
        query = Query(property="bbox", operator=operations)
        assert query is not None
        assert query.property == "bbox"
        assert query.operator == excepted_operations

    def test_single_query_dict(self):
        query = Query(property="datetime", operator=[("eq", "2020-01-01")])
        assert query is not None
        assert query.dict() == {"datetime": {"eq": "2020-01-01"}}

    def test_multiple_query_dict(self):
        query = Query(
            property="datetime", operator=[(">", "2020-01-01"), ("lt", "2020-01-02")]
        )
        assert query is not None
        assert query.dict() == {"datetime": {"gt": "2020-01-01", "lt": "2020-01-02"}}

    def test_fail_query(self):
        with pytest.raises(ValueError) as excinfo:
            Query(property={"bbox": 1}, operator=2)
        assert "property" in str(excinfo.value)
        assert "operator" in str(excinfo.value)
        assert "value is not a valid" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Query(property="bbox", operator=2)

        assert "operator" in str(excinfo.value)
        assert "value is not a valid" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Query(property="bbox", operator=[2])

        assert "operator" in str(excinfo.value)
        assert "value is not a valid" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Query(property={"bbox": 1}, operator=[("eq", 2)])

        assert "property" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Query(property="bbox", operator=[("eq", 2), 2])

        assert "operator" in str(excinfo.value)
        assert "value is not a valid" in str(excinfo.value)

    def test_fail_opeartion_query(self):
        operator = _QUERY_OPERATOR + list(_OPERATOR_MAP.keys())
        wrong_operator = "wrong"
        assert wrong_operator not in operator

        with pytest.raises(ValueError) as excinfo:
            Query(property="bbox", operator=[(wrong_operator, 2)])

        assert f"""Operator {wrong_operator} is not valid""" in str(excinfo.value)


class TestFilter:
    """
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
    Need to have versions:
        - dict
    """

    def test_single_filter(self):
        filter = Filter(
            op="and", args=[{"op": "=", "args": ["datetime", "2020-01-01"]}]
        )
        assert filter is not None
        assert filter.op == "and"
        assert filter.args == [{"op": "=", "args": ["datetime", "2020-01-01"]}]

    def test_multiple_filter(self):
        args = [
            {"op": "=", "args": [{"property": "collection"}, "landsat8_l1tp"]},
            {"op": "<=", "args": [{"property": "eo:cloud_cover"}, 10]},
        ]
        filter = Filter(op="or", args=args)

        assert filter is not None
        assert filter.op == "or"
        assert filter.args == args

    def test_single_filter_dict(self):
        filter = Filter(
            op="and", args=[{"op": "=", "args": ["datetime", "2020-01-01"]}]
        )

        assert filter is not None
        assert filter.dict() == {
            "op": "and",
            "args": [{"op": "=", "args": ["datetime", "2020-01-01"]}],
        }

    def test_multiple_filter_dict(self):
        args = [
            {"op": "=", "args": [{"property": "collection"}, "landsat8_l1tp"]},
            {"op": "<=", "args": [{"property": "eo:cloud_cover"}, 10]},
        ]
        filter = Filter(op="or", args=args)

        assert filter is not None
        assert filter.dict() == {"op": "or", "args": args}

    def test_fail_filter(self):
        with pytest.raises(ValueError) as excinfo:
            Filter(op="op", args="args")

        assert "args" in str(excinfo.value)
        assert "value is not a valid" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Filter(op=[0], args="args")

        assert "op" in str(excinfo.value)
        assert "value is not a valid" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Filter(op="op", args=[0])

        assert "args" in str(excinfo.value)
        assert "value is not a valid" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Filter(op="op", args=[{None: 1}])

        assert "args" in str(excinfo.value)


class TestSortBy:
    """
    The schema looks like this:
        {
            "field": "<property_name>",
            "direction": "<direction>"
        }
    Need to have versions:
        - dict
        - str
    """

    def test_single_sortby(self):
        sortby = SortBy(field="datetime", direction="asc")
        assert sortby is not None
        assert sortby.field == "datetime"
        assert sortby.direction == "asc"
        sortby = SortBy(field="bbox", direction="desc")
        assert sortby is not None
        assert sortby.field == "bbox"
        assert sortby.direction == "desc"

    def test_single_sortby_dict(self):
        sortby = SortBy(field="datetime", direction="asc")
        assert sortby is not None
        assert sortby.dict() == {"field": "datetime", "direction": "asc"}
        sortby = SortBy(field="bbox", direction="desc")
        assert sortby is not None
        assert sortby.dict() == {"field": "bbox", "direction": "desc"}

    def test_single_sortby_str(self):
        sortby = SortBy(field="datetime", direction="asc")
        assert sortby is not None
        assert str(sortby) == "+datetime"
        sortby = SortBy(field="bbox", direction="desc")
        assert sortby is not None
        assert str(sortby) == "-bbox"

    def test_from_string_sortby(self):
        single_str_asc = "+datetime"
        sortby_list = SortBy.from_string(single_str_asc)
        assert sortby_list is not None
        assert len(sortby_list) == 1
        sortby = sortby_list[0]
        assert sortby.field == "datetime"
        assert sortby.direction == "asc"

        single_str_asc = "datetime"
        sortby_list = SortBy.from_string(single_str_asc)
        assert sortby_list is not None
        assert len(sortby_list) == 1
        sortby = sortby_list[0]
        assert sortby.field == "datetime"
        assert sortby.direction == "asc"

        single_str_desc = "-cloud_cover"
        sortby_list = SortBy.from_string(single_str_desc)
        assert sortby_list is not None
        assert len(sortby_list) == 1
        sortby = sortby_list[0]
        assert sortby.field == "cloud_cover"
        assert sortby.direction == "desc"

        single_str = "+datetime,-cloud_cover"
        sortby_list = SortBy.from_string(single_str)
        assert sortby_list is not None
        assert len(sortby_list) == 2
        sortby = sortby_list[0]
        assert sortby.field == "datetime"
        assert sortby.direction == "asc"
        sortby = sortby_list[1]
        assert sortby.field == "cloud_cover"
        assert sortby.direction == "desc"

        single_str = "datetime,-cloud_cover"
        sortby_list = SortBy.from_string(single_str)
        assert sortby_list is not None
        assert len(sortby_list) == 2
        sortby = sortby_list[0]
        assert sortby.field == "datetime"
        assert sortby.direction == "asc"
        sortby = sortby_list[1]
        assert sortby.field == "cloud_cover"
        assert sortby.direction == "desc"

        multiple_str_asc = "+datetime,cloud_cover"
        sortby_list = SortBy.from_string(multiple_str_asc)
        assert sortby_list is not None
        assert len(sortby_list) == 2
        sortby = sortby_list[0]
        assert sortby.field == "datetime"
        assert sortby.direction == "asc"
        sortby = sortby_list[1]
        assert sortby.field == "cloud_cover"
        assert sortby.direction == "asc"

        multiple_str_desc = "-datetime,-cloud_cover"
        sortby_list = SortBy.from_string(multiple_str_desc)
        assert sortby_list is not None
        assert len(sortby_list) == 2
        sortby = sortby_list[0]
        assert sortby.field == "datetime"
        assert sortby.direction == "desc"
        sortby = sortby_list[1]
        assert sortby.field == "cloud_cover"
        assert sortby.direction == "desc"

        multiple_str = "+datetime,cloud_cover,-bbox"
        sortby_list = SortBy.from_string(multiple_str)
        assert sortby_list is not None
        assert len(sortby_list) == 3
        sortby = sortby_list[0]
        assert sortby.field == "datetime"
        assert sortby.direction == "asc"
        sortby = sortby_list[1]
        assert sortby.field == "cloud_cover"
        assert sortby.direction == "asc"
        sortby = sortby_list[2]
        assert sortby.field == "bbox"
        assert sortby.direction == "desc"

        multiple_str = "datetime,-cloud_cover,-bbox"
        sortby_list = SortBy.from_string(multiple_str)
        assert sortby_list is not None
        assert len(sortby_list) == 3
        sortby = sortby_list[0]
        assert sortby.field == "datetime"
        assert sortby.direction == "asc"
        sortby = sortby_list[1]
        assert sortby.field == "cloud_cover"
        assert sortby.direction == "desc"
        sortby = sortby_list[2]
        assert sortby.field == "bbox"
        assert sortby.direction == "desc"

    def test_fail_sortby(self):
        with pytest.raises(ValueError) as excinfo:
            SortBy(field="datetime", direction="ascend")

        assert "direction" in str(excinfo.value)
        assert "ascend" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            SortBy(field=None, direction="ascend")

        assert "field" in str(excinfo.value)
        assert "none" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            SortBy(field="datetime", direction=None)

        assert "direction" in str(excinfo.value)
        assert "none" in str(excinfo.value)

        with pytest.raises(EmptyAttributeError) as excinfo:
            SortBy.from_string(None)

        assert "Empty string provided" in str(excinfo.value)

        with pytest.raises(EmptyAttributeError) as excinfo:
            SortBy.from_string("")

        assert "Empty string provided" in str(excinfo.value)


class TestField:
    """
    The schema looks like this:
        {
            "include": ["<property1>", "<property2>", ...],
            "exclude": ["<property1>", "<property2>", ...]
        }
    Need to have versions:
        - dict
        - str
    """

    def test_single_field(self):
        field = Field(field_type="include", fields=["datetime"])
        assert field is not None
        assert field.field_type == "include"
        assert field.fields == ["datetime"]

        field = Field(field_type="exclude", fields=["datetime"])
        assert field is not None
        assert field.field_type == "exclude"
        assert field.fields == ["datetime"]

    def test_multiple_field(self):
        field = Field(field_type="include", fields=["datetime", "cloud_cover"])
        assert field is not None
        assert field.field_type == "include"
        assert field.fields == ["datetime", "cloud_cover"]

        field = Field(field_type="exclude", fields=["datetime", "cloud_cover"])
        assert field is not None
        assert field.field_type == "exclude"
        assert field.fields == ["datetime", "cloud_cover"]

    def test_single_field_dict(self):
        field = Field(field_type="include", fields=["datetime"])
        assert field is not None
        assert field.dict() == {"include": ["datetime"]}

        field = Field(field_type="exclude", fields=["datetime"])
        assert field is not None
        assert field.dict() == {"exclude": ["datetime"]}

    def test_multiple_field_dict(self):
        field = Field(field_type="include", fields=["datetime", "cloud_cover"])
        assert field is not None
        assert field.dict() == {"include": ["datetime", "cloud_cover"]}

        field = Field(field_type="exclude", fields=["datetime", "cloud_cover"])
        assert field is not None
        assert field.dict() == {"exclude": ["datetime", "cloud_cover"]}

    def test_single_field_str(self):
        field = Field(field_type="include", fields=["datetime"])
        assert field is not None
        assert str(field) == "+datetime"

        field = Field(field_type="exclude", fields=["datetime"])
        assert field is not None
        assert str(field) == "-datetime"

    def test_multiple_field_str(self):
        field = Field(field_type="include", fields=["datetime", "cloud_cover"])
        assert field is not None
        assert str(field) == "+datetime,+cloud_cover"

        field = Field(field_type="exclude", fields=["datetime", "cloud_cover"])
        assert field is not None
        assert str(field) == "-datetime,-cloud_cover"

    def test_from_string_field(self):
        single_str_asc = "+datetime"
        field_list = Field.from_string(single_str_asc)
        assert field_list is not None
        assert len(field_list) == 2
        assert field_list[0] is not None
        assert field_list[1] is None
        field = field_list[0]
        assert field.fields == ["datetime"]
        assert field.field_type == "include"

        single_str_asc = "datetime"
        field_list = Field.from_string(single_str_asc)
        assert field_list is not None
        assert len(field_list) == 2
        assert field_list[0] is not None
        assert field_list[1] is None
        field = field_list[0]
        assert field.fields == ["datetime"]
        assert field.field_type == "include"

        single_str_desc = "-cloud_cover"
        field_list = Field.from_string(single_str_desc)
        assert field_list is not None
        assert len(field_list) == 2
        assert field_list[0] is None
        assert field_list[1] is not None
        field = field_list[1]
        assert field.fields == ["cloud_cover"]
        assert field.field_type == "exclude"

        single_str = "+datetime,-cloud_cover"
        field_list = Field.from_string(single_str)
        assert field_list is not None
        assert len(field_list) == 2
        assert field_list[0] is not None
        assert field_list[1] is not None
        field = field_list[0]
        assert field.fields == ["datetime"]
        assert field.field_type == "include"
        field = field_list[1]
        assert field.fields == ["cloud_cover"]
        assert field.field_type == "exclude"

        single_str = "datetime,-cloud_cover"
        field_list = Field.from_string(single_str)
        assert field_list is not None
        assert len(field_list) == 2
        assert field_list[0] is not None
        assert field_list[1] is not None
        field = field_list[0]
        assert field.fields == ["datetime"]
        assert field.field_type == "include"
        field = field_list[1]
        assert field.fields == ["cloud_cover"]
        assert field.field_type == "exclude"

        multiple_str_asc = "+datetime,cloud_cover"
        field_list = Field.from_string(multiple_str_asc)
        assert field_list is not None
        assert len(field_list) == 2
        assert field_list[0] is not None
        assert field_list[1] is None
        field = field_list[0]
        assert field.fields == ["datetime", "cloud_cover"]
        assert field.field_type == "include"

        multiple_str_desc = "-datetime,-cloud_cover"
        field_list = Field.from_string(multiple_str_desc)
        assert field_list is not None
        assert len(field_list) == 2
        assert field_list[0] is None
        assert field_list[1] is not None
        field = field_list[1]
        assert field.fields == ["datetime", "cloud_cover"]
        assert field.field_type == "exclude"

        multiple_str = "+datetime,cloud_cover,-bbox"
        field_list = Field.from_string(multiple_str)
        assert field_list is not None
        assert len(field_list) == 2
        assert field_list[0] is not None
        assert field_list[1] is not None
        field = field_list[0]
        assert field.fields == ["datetime", "cloud_cover"]
        assert field.field_type == "include"
        field = field_list[1]
        assert field.fields == ["bbox"]
        assert field.field_type == "exclude"

        multiple_str = "datetime,-cloud_cover,-bbox"
        field_list = Field.from_string(multiple_str)
        assert field_list is not None
        assert len(field_list) == 2
        assert field_list[0] is not None
        assert field_list[1] is not None
        field = field_list[0]
        assert field.fields == ["datetime"]
        assert field.field_type == "include"
        field = field_list[1]
        assert field.fields == ["cloud_cover", "bbox"]
        assert field.field_type == "exclude"

    def test_fail_field(self):
        with pytest.raises(ValueError) as excinfo:
            Field(field_type="append", fields=["datetime"])

        assert "field_type" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Field(field_type=None, fields=["datetime"])

        assert "field_type" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Field(field_type="include", fields="datetime")

        assert "fields" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Field(field_type="include", fields=None)

        assert "fields" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Field(field_type="include", fields=["datetime", None])

        assert "fields" in str(excinfo.value)

        with pytest.raises(EmptyAttributeError) as excinfo:
            Field.from_string(None)

        assert "Empty string provided" in str(excinfo.value)

        with pytest.raises(EmptyAttributeError) as excinfo:
            Field.from_string("")

        assert "Empty string provided" in str(excinfo.value)


class TestSearch:
    def test_default_search(self):
        search = Search(search_type="api")
        assert search is not None
        assert search.search_type == "api"
        assert search.bbox is None
        assert search.intersects is None
        assert search.datetime is None
        assert search.ids is None
        assert search.collections is None
        assert search.query is None
        assert search.filter is None
        assert search.filter_lang is None
        assert search.sort_by is None
        assert search.fields is None
        assert search.limit == DEFAUL_LIMIT

        with pytest.raises(FrozenInstanceError) as excinfo:
            search.search_type = "static"

        assert "search_type" in str(excinfo.value)

    def test_to_utc_isoformat(self):
        search = Search(search_type="api")

        dt = datetime(2022, 3, 15, 12, 30, 45, tzinfo=timezone.utc)
        output = search._to_utc_isoformat(dt)
        assert output == "2022-03-15T12:30:45Z"

        dt = datetime(2022, 3, 15, 12, 30, 45, tzinfo=timezone(timedelta(hours=1)))
        output = search._to_utc_isoformat(dt)
        assert output == "2022-03-15T11:30:45Z"

        dt = datetime(2022, 3, 15, 11, 30, 45, tzinfo=timezone(timedelta(hours=-1)))
        output = search._to_utc_isoformat(dt)
        assert output == "2022-03-15T12:30:45Z"

    def test_datetime_to_range(self):
        single_year = "2022"
        output = Search._datetime_to_range(single_year)
        assert output == (
            datetime(2022, 1, 1, tzinfo=timezone.utc),
            datetime(2022, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        )

        single_year_month = "2022-03"
        output = Search._datetime_to_range(single_year_month)
        assert output == (
            datetime(2022, 3, 1, tzinfo=timezone.utc),
            datetime(2022, 3, 31, 23, 59, 59, tzinfo=timezone.utc),
        )

        single_year_month_day = "2022-03-15"
        output = Search._datetime_to_range(single_year_month_day)
        assert output == (
            datetime(2022, 3, 15, tzinfo=timezone.utc),
            datetime(2022, 3, 15, 23, 59, 59, tzinfo=timezone.utc),
        )

        single_reminder = "2022-03-15T12:30:45"
        output = Search._datetime_to_range(single_reminder)
        assert output == (datetime(2022, 3, 15, 12, 30, 45, tzinfo=timezone.utc), None)

        single_reminder_tz_info = "2022-03-15T12:30:45+01:00"
        output = Search._datetime_to_range(single_reminder_tz_info)
        assert output == (datetime(2022, 3, 15, 11, 30, 45, tzinfo=timezone.utc), None)

    def test_search_type(self):
        search = Search(search_type="api")
        assert search is not None
        assert search.search_type is not None
        assert search.search_type == "api"

        search = Search(search_type="static")
        assert search is not None
        assert search.search_type is not None
        assert search.search_type == "static"

        with pytest.raises(ValueError) as excinfo:
            Search(search_type=None)

        assert "search_type" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Search(search_type="invalid")

        assert "search_type" in str(excinfo.value)

    def test_bbox(self):
        search = Search(search_type="api", bbox=[-180, -90, 180, 90])
        assert search is not None
        assert search.bbox is not None
        assert search.bbox == (-180.0, -90.0, 180.0, 90.0)

        search = Search(search_type="api", bbox=[-180, -90, 180, 90, 0, 100])
        assert search is not None
        assert search.bbox is not None
        assert search.bbox == (-180.0, -90.0, 180.0, 90.0, 0.0, 100.0)

        search = Search(search_type="api", bbox=[-180.0, -90.0, 180.0, 90.0])
        assert search is not None
        assert search.bbox is not None
        assert search.bbox == (-180.0, -90.0, 180.0, 90.0)

        search = Search(
            search_type="api", bbox=[-180.0, -90.0, 180.0, 90.0, 0.0, 100.0]
        )
        assert search is not None
        assert search.bbox is not None
        assert search.bbox == (-180.0, -90.0, 180.0, 90.0, 0.0, 100.0)

        with pytest.raises(ValueError) as excinfo:
            Search(search_type="api", bbox=[-180, -90, 180])

        assert "bbox" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Search(search_type="api", bbox=[-180, -90, 180, 90, 0])

        assert "bbox" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Search(search_type="api", bbox=[-180, -90, 180, 90, 0, 100, 200])

        assert "bbox" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Search(search_type="api", bbox=[-180, -90, 180, 90, 0, 100, 200, "a"])

        assert "bbox" in str(excinfo.value)

        search = Search(
            search_type="api",
            bbox=[-180, -90, 180, 90],
            intersects={
                "type": "Polygon",
                "coordinates": [
                    [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
                ],
            },
        )
        assert search is not None
        assert search.bbox is None
        assert search.intersects is not None

    def test_intersects(self):
        search = Search(
            search_type="api",
            intersects={
                "type": "Polygon",
                "coordinates": [
                    [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
                ],
            },
        )
        assert search is not None
        assert search.intersects is not None
        assert search.intersects == {
            "type": "Polygon",
            "coordinates": [
                [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
            ],
        }
        assert isinstance(search.intersects, GeoJSON)

        search = Search(
            search_type="api",
            intersects={
                "type": "LineString",
                "coordinates": [
                    [-180, -90],
                    [180, -90],
                    [180, 90],
                    [-180, 90],
                    [-180, -90],
                ],
            },
        )
        assert search is not None
        assert search.intersects is not None
        assert search.intersects == {
            "type": "LineString",
            "coordinates": [
                [-180, -90],
                [180, -90],
                [180, 90],
                [-180, 90],
                [-180, -90],
            ],
        }

        search = Search(
            search_type="api", intersects={"type": "Point", "coordinates": [-180, -90]}
        )
        assert search is not None
        assert search.intersects is not None
        assert search.intersects == {"type": "Point", "coordinates": [-180, -90]}

        coordinates = [
            [
                [
                    [-122.422244, 37.747969],
                    [-122.422244, 37.752421],
                    [-122.415424, 37.752421],
                    [-122.415424, 37.747969],
                    [-122.422244, 37.747969],
                ]
            ],
            [
                [
                    [-122.419483, 37.749040],
                    [-122.419483, 37.751608],
                    [-122.416858, 37.751608],
                    [-122.416858, 37.749040],
                    [-122.419483, 37.749040],
                ]
            ],
        ]
        search = Search(
            search_type="api",
            intersects={"type": "MultiPolygon", "coordinates": coordinates},
        )
        assert search is not None
        assert search.intersects is not None
        assert search.intersects == {"type": "MultiPolygon", "coordinates": coordinates}

        coordinates = [
            [[-73.989, 40.733], [-73.99, 40.733], [-73.99, 40.734], [-73.989, 40.734]],
            [[-73.988, 40.732], [-73.989, 40.732]],
        ]
        search = Search(
            search_type="api",
            intersects={"type": "MultiLineString", "coordinates": coordinates},
        )
        assert search is not None
        assert search.intersects is not None
        assert search.intersects == {
            "type": "MultiLineString",
            "coordinates": coordinates,
        }

        search = Search(
            search_type="api",
            intersects={
                "type": "MultiPoint",
                "coordinates": [[-73, 40], [-73.99, 40.734], [-73, 40]],
            },
        )
        assert search is not None
        assert search.intersects is not None
        assert search.intersects == {
            "type": "MultiPoint",
            "coordinates": [[-73, 40], [-73.99, 40.734], [-73, 40]],
        }

        with pytest.raises(ValueError) as excinfo:
            Search(
                search_type="api",
                intersects={"type": "Polygon", "coordinates": [[[-180, -90]]]},
            )

        assert "intersects" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Search(
                search_type="api",
                intersects={
                    "type": "Pine",
                    "coordinates": [
                        [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
                    ],
                },
            )

        assert "intersects" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Search(search_type="api", intersects={})

        assert "intersects" in str(excinfo.value)

    def test_datetime(self):
        single_year = "2018"
        search = Search(search_type="api", datetime=single_year)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2018, 12, 31, 23, 59, 59, tzinfo=timezone.utc
        )

        single_year_month = "2018-01"
        search = Search(search_type="api", datetime=single_year_month)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2018, 1, 31, 23, 59, 59, tzinfo=timezone.utc
        )

        single_year_month_day = "2018-01-01"
        search = Search(search_type="api", datetime=single_year_month_day)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2018, 1, 1, 23, 59, 59, tzinfo=timezone.utc
        )

        single_reminder = "2018-01-01T00:00:00"
        search = Search(search_type="api", datetime=single_reminder)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[1] is None
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        single_reminder_with_tz = "2018-01-01T10:00:00+10:00"
        search = Search(search_type="api", datetime=single_reminder_with_tz)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[1] is None
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        datetime_object = datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        search = Search(search_type="api", datetime=datetime_object)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[1] is None
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        datetime_object_tuple = (
            datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2018, 1, 1, 23, 59, 59, tzinfo=timezone.utc),
        )
        search = Search(search_type="api", datetime=datetime_object_tuple)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2018, 1, 1, 23, 59, 59, tzinfo=timezone.utc
        )

        range_year = "2018/2019"
        search = Search(search_type="api", datetime=range_year)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2019, 12, 31, 23, 59, 59, tzinfo=timezone.utc
        )

        range_year_month = "2018-01/2019-01"
        search = Search(search_type="api", datetime=range_year_month)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2019, 1, 31, 23, 59, 59, tzinfo=timezone.utc
        )

        range_year_month = "2018-01/2019"
        search = Search(search_type="api", datetime=range_year_month)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2019, 12, 31, 23, 59, 59, tzinfo=timezone.utc
        )

        range_year_month = "2018/2019-01"
        search = Search(search_type="api", datetime=range_year_month)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2019, 1, 31, 23, 59, 59, tzinfo=timezone.utc
        )

        range_year_month_day = "2018-01-01/2019-01-01"
        search = Search(search_type="api", datetime=range_year_month_day)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2019, 1, 1, 23, 59, 59, tzinfo=timezone.utc
        )

        range_year_month_day = "2018-01-01/2019-01"
        search = Search(search_type="api", datetime=range_year_month_day)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2019, 1, 31, 23, 59, 59, tzinfo=timezone.utc
        )

        range_year_month_day = "2018-01/2019-01-01"
        search = Search(search_type="api", datetime=range_year_month_day)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2019, 1, 1, 23, 59, 59, tzinfo=timezone.utc
        )

        range_reminder = "2018-01-01T10:00:00/2019-01-01T22:59:59"
        search = Search(search_type="api", datetime=range_reminder)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2019, 1, 1, 22, 59, 59, tzinfo=timezone.utc
        )

        range_reminder_with_tz = "2018-01-01T10:00:00+01:00/2019-01-01T22:59:59+01:00"
        search = Search(search_type="api", datetime=range_reminder_with_tz)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0].astimezone(timezone.utc) == datetime(
            2018, 1, 1, 9, 0, 0, tzinfo=timezone.utc
        )
        assert search.datetime[1].astimezone(timezone.utc) == datetime(
            2019, 1, 1, 21, 59, 59, tzinfo=timezone.utc
        )

        range_flip_year = "2019/2018"
        search = Search(search_type="api", datetime=range_flip_year)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert search.datetime[1] == datetime(
            2019, 12, 31, 23, 59, 59, tzinfo=timezone.utc
        )

        range_tuple = (datetime(2018, 1, 1), None)
        search = Search(search_type="api", datetime=range_tuple)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2018, 1, 1, 0, 0, 0)
        assert search.datetime[1] is None

        range_tuple = (datetime(2020, 1, 1), datetime(2019, 1, 1))
        search = Search(search_type="api", datetime=range_tuple)
        assert search is not None
        assert search.datetime is not None
        assert len(search.datetime) == 2
        assert search.datetime[0] == datetime(2019, 1, 1, 0, 0, 0)
        assert search.datetime[1] == datetime(2020, 1, 1, 0, 0, 0)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", datetime="2018.01.01")

        assert "invalid datetime component" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", datetime="2018-01-01/nothing")

        assert "datetime" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(
                search_type="api", datetime="2018-01-01/2019-01-01/2019-01-01"
            )

        assert "datetime range must be max 2" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(
                search_type="api",
                datetime=(
                    datetime(2019, 1, 1),
                    datetime(2019, 1, 1),
                    datetime(2019, 1, 1),
                ),
            )

        assert "datetime range must be max 2" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", datetime=(None, datetime(2019, 1, 1)))

        assert "invalid datetime tuple, start must be a datetime object" in str(
            excinfo.value
        )

        with pytest.raises(ValueError) as excinfo:
            search = Search(
                search_type="api", datetime=("2018-01-01", datetime(2019, 1, 1))
            )

        assert "invalid datetime tuple, start must be a datetime object" in str(
            excinfo.value
        )

        with pytest.raises(ValueError) as excinfo:
            search = Search(
                search_type="api", datetime=(datetime(2019, 1, 1), "2019-01-01")
            )

        assert "invalid datetime tuple, end must be a datetime object" in str(
            excinfo.value
        )

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", datetime=1)

        assert "datetime " in str(excinfo.value)

    def test_ids(self):
        search = Search(search_type="api", ids="1")
        assert search is not None
        assert search.ids is not None
        assert search.ids == ["1"]

        search = Search(search_type="api", ids="1,2,3")
        assert search is not None
        assert search.ids is not None
        assert search.ids == ["1", "2", "3"]

        search = Search(search_type="api", ids=["1", "2", "3"])
        assert search is not None
        assert search.ids is not None
        assert search.ids == ["1", "2", "3"]

        search = Search(search_type="api", ids=[1, 2, 3])
        assert search is not None
        assert search.ids is not None
        assert search.ids == ["1", "2", "3"]

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", ids=1)

        assert "ids" in str(excinfo.value)

    def test_collection(self):
        search = Search(search_type="api", collections="collection")
        assert search is not None
        search.collections is not None
        assert search.collections == ["collection"]

        search = Search(
            search_type="api", collections="collection1,collection2,collection3"
        )
        assert search is not None
        search.collections is not None
        assert search.collections == ["collection1", "collection2", "collection3"]

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", collections=1)

        assert "collections" in str(excinfo.value)

    def test_query(self):
        query = Query(property="bbox", operator=[("eq", 2)])
        search = Search(search_type="api", query=query)
        assert search is not None
        assert search.query is not None
        assert search.query == [query]

        query = [
            Query(property="bbox", operator=[("eq", 2)]),
            Query(property="bbox", operator=[("eq", 2)]),
        ]

        search = Search(search_type="api", query=query)
        assert search is not None
        assert search.query is not None
        assert search.query == query

        query = ("bbox", [("eq", 2)])
        search = Search(search_type="api", query=query)
        assert search is not None
        assert search.query is not None
        assert search.query == [Query(property="bbox", operator=[("eq", 2)])]

        query = [("bbox", [("eq", 2)]), ("bbox", [("eq", 2)])]
        search = Search(search_type="api", query=query)
        assert search is not None
        assert search.query is not None
        assert search.query == [
            Query(property="bbox", operator=[("eq", 2)]),
            Query(property="bbox", operator=[("eq", 2)]),
        ]

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", query=1)

        assert "query" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", query=[1])

        assert "query" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", query=[("bbox")])

        assert "query" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(
                search_type="api", query=[("bbox", [("eq", 2)]), ("bbox", 1)]
            )

        assert "query" in str(excinfo.value)

    def test_filter(self):
        filter = Filter(
            op="and", args=[{"op": "=", "args": ["datetime", "2020-01-01"]}]
        )
        search = Search(search_type="api", filter=filter)
        assert search is not None
        assert search.filter is not None
        assert search.filter_lang is not None
        assert search.filter_lang == "cql2-json"
        assert search.filter == filter

        filter = ("and", [{"op": "=", "args": ["datetime", "2020-01-01"]}])
        search = Search(search_type="api", filter=filter)
        assert search is not None
        assert search.filter is not None
        assert search.filter_lang is not None
        assert search.filter_lang == "cql2-json"
        assert search.filter == Filter(
            op="and", args=[{"op": "=", "args": ["datetime", "2020-01-01"]}]
        )

        filter = ["and", [{"op": "=", "args": ["datetime", "2020-01-01"]}]]
        search = Search(search_type="api", filter=filter)
        assert search is not None
        assert search.filter is not None
        assert search.filter_lang is not None
        assert search.filter_lang == "cql2-json"
        assert search.filter == Filter(
            op="and", args=[{"op": "=", "args": ["datetime", "2020-01-01"]}]
        )

        filter = (
            "filter=id='LC08_L1TP_060247_20180905_20180912_01_T1_L1TP'"
            "AND collection='landsat8_l1tp'"
        )
        search = Search(search_type="api", filter=filter)
        assert search is not None
        assert search.filter is not None
        assert search.filter_lang is not None
        assert search.filter_lang == "cql2-text"
        assert search.filter == filter

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", filter=1)

        assert "filter" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", filter=["and"])

        assert "filter" in str(excinfo.value)

    def test_sort(self):
        sort = SortBy(field="datetime", direction="asc")
        search = Search(search_type="api", sort_by=sort)
        assert search is not None
        assert search.sort_by is not None
        assert search.sort_by == [sort]

        sort = [
            SortBy(field="datetime", direction="asc"),
            SortBy(field="datetime", direction="asc"),
        ]
        search = Search(search_type="api", sort_by=sort)
        assert search is not None
        assert search.sort_by is not None
        assert search.sort_by == sort

        sort = [("asc", "datetime")]
        search = Search(search_type="api", sort_by=sort)
        assert search is not None
        assert search.sort_by is not None
        assert search.sort_by == [SortBy(field="datetime", direction="asc")]

        sort = [("asc", "datetime"), ("desc", "datetime")]
        search = Search(search_type="api", sort_by=sort)
        assert search is not None
        assert search.sort_by is not None
        assert search.sort_by == [
            SortBy(field="datetime", direction="asc"),
            SortBy(field="datetime", direction="desc"),
        ]

        sort = [("asc", "datetime,datetime")]
        search = Search(search_type="api", sort_by=sort)
        assert search is not None
        assert search.sort_by is not None
        assert search.sort_by == [
            SortBy(field="datetime", direction="asc"),
            SortBy(field="datetime", direction="asc"),
        ]

        sort = [("asc", "datetime"), ("desc", ["datetime", "datetime"])]

        sort = "+datetime"
        search = Search(search_type="api", sort_by=sort)
        assert search is not None
        assert search.sort_by is not None
        assert search.sort_by == [SortBy(field="datetime", direction="asc")]

        sort = "+datetime,-datetime"
        search = Search(search_type="api", sort_by=sort)
        assert search is not None
        assert search.sort_by is not None
        assert search.sort_by == [
            SortBy(field="datetime", direction="asc"),
            SortBy(field="datetime", direction="desc"),
        ]

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", sort_by=1)

        assert "sort_by" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", sort_by=[1])

        assert "sort_by" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", sort_by=[("asc")])

        assert "sort_by" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", sort_by=[("asc", "datetime"), 1])

        assert "sort_by" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", sort_by=[("asc", {"datetime": "asc"})])

        assert "sort_by" in str(excinfo.value)

    def test_fields(self):
        field = Field(field_type="include", fields=["datetime"])
        search = Search(search_type="api", fields=field)
        assert search is not None
        assert search.fields is not None
        assert search.fields == (field, None)

        field = Field(field_type="exclude", fields=["datetime"])
        search = Search(search_type="api", fields=field)
        assert search is not None
        assert search.fields is not None
        assert search.fields == (None, field)

        field = "+datetime,-datetime"
        search = Search(search_type="api", fields=field)
        assert search is not None
        assert search.fields is not None
        assert search.fields == (
            Field(field_type="include", fields=["datetime"]),
            Field(field_type="exclude", fields=["datetime"]),
        )

        field = "+datetime"
        search = Search(search_type="api", fields=field)
        assert search is not None
        assert search.fields is not None
        assert search.fields == (Field(field_type="include", fields=["datetime"]), None)

        field = "-datetime"
        search = Search(search_type="api", fields=field)
        assert search is not None
        assert search.fields is not None
        assert search.fields == (None, Field(field_type="exclude", fields=["datetime"]))

        field = (
            Field(field_type="include", fields=["datetime"]),
            Field(field_type="exclude", fields=["datetime"]),
        )
        search = Search(search_type="api", fields=field)
        assert search is not None
        assert search.fields is not None
        assert search.fields == field

        field = ("include", ["datetime"])
        search = Search(search_type="api", fields=field)
        assert search is not None
        assert search.fields is not None
        assert search.fields == (Field(field_type="include", fields=["datetime"]), None)

        field = ("exclude", ["datetime"])
        search = Search(search_type="api", fields=field)
        assert search is not None
        assert search.fields is not None
        assert search.fields == (None, Field(field_type="exclude", fields=["datetime"]))

        field = [("include", ["datetime"]), ("exclude", ["datetime"])]
        search = Search(search_type="api", fields=field)
        assert search is not None
        assert search.fields is not None
        assert search.fields == (
            Field(field_type="include", fields=["datetime"]),
            Field(field_type="exclude", fields=["datetime"]),
        )

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", fields=1)

        assert "fields" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", fields=[1])

        assert "fields" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", fields=[("include")])

        assert "fields" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(search_type="api", fields=[("include", "datetime"), 1])

        assert "fields" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(
                search_type="api",
                fields=[("include", ["datetime"]), ("include", ["datetime"]), 1],
            )

        assert "fields" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(
                search_type="api",
                fields=[("exclude", ["datetime"]), ("exclude", ["datetime"]), 1],
            )

        assert "fields" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(
                search_type="api",
                fields=[
                    Field(field_type="include", fields=["datetime"]),
                    Field(field_type="include", fields=["datetime"]),
                ],
            )

        assert "fields" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(
                search_type="api",
                fields=[
                    Field(field_type="exclude", fields=["datetime"]),
                    Field(field_type="include", fields=["datetime"]),
                    Field(field_type="include", fields=["datetime"]),
                ],
            )

        assert "fields" in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            search = Search(
                search_type="api",
                fields=[
                    Field(field_type="exclude", fields=["datetime"]),
                    ("include", "datetime", "Wrong"),
                ],
            )

        assert "fields" in str(excinfo.value)

    def test_to_dict(self):
        search = Search(
            search_type="api",
        )
        assert search is not None
        assert search.dict() == {"search_type": "api", "limit": DEFAUL_LIMIT}

        search = Search(
            search_type="api",
            bbox=[-180, -90, 180, 90],
            ids=[1, 2, 3],
            collections=["collection1", "collection2"],
            query=[
                Query(property="datetime", operator=[("eq", 2)]),
                Query(property="bbox", operator=[("eq", 4)]),
            ],
            sort_by=[
                SortBy(field="datetime", direction="asc"),
                SortBy(field="datetime", direction="desc"),
            ],
        )
        assert search is not None
        dict_search = search.dict()
        assert "bbox" in dict_search
        dict_search["bbox"] == search.bbox
        assert "ids" in dict_search
        dict_search["ids"] == search.ids
        assert "collections" in dict_search
        dict_search["collections"] == search.collections
        assert "query" in dict_search
        dict_search["query"] == {"datetime": {"eq": 2}, "bbox": {"eq": 4}}
        assert "sortby" in dict_search
        dict_search["sortby"] == [
            {"field": "datetime", "direction": "asc"},
            {"field": "datetime", "direction": "desc"},
        ]
        assert "limit" in dict_search
        dict_search["limit"] == DEFAUL_LIMIT

        intersect = {
            "type": "Polygon",
            "coordinates": [
                [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
            ],
        }
        search = Search(search_type="api", intersects=intersect)
        assert search is not None
        dict_search = search.dict()
        assert "intersects" in dict_search
        dict_search["intersects"] == intersect

        search = Search(
            search_type="api",
            datetime=(datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc), None),
        )
        assert search is not None
        dict_search = search.dict()
        assert "datetime" in dict_search
        dict_search["datetime"] == "2018-01-01T00:00:00Z"

        search = Search(
            search_type="api",
            datetime=(
                datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2019, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            ),
        )
        assert search is not None
        dict_search = search.dict()
        assert "datetime" in dict_search
        dict_search["datetime"] == "2018-01-01T00:00:00Z/2019-01-01T00:00:00Z"

        search = Search(
            search_type="api",
            filter=Filter(
                op="and", args=[{"op": "=", "args": ["datetime", "2020-01-01"]}]
            ),
        )
        assert search is not None
        dict_search = search.dict()
        assert "filter-lang" in dict_search
        dict_search["filter-lang"] == "cql2-json"
        assert "filter" in dict_search
        dict_search["filter"] == {
            "op": "and",
            "args": [{"op": "=", "args": ["datetime", "2020-01-01"]}],
        }

        search = Search(
            search_type="api",
            filter=(
                "filter=id='LC08_L1TP_060247_20180905_20180912_01_T1_L1TP'"
                "AND collection='landsat8_l1tp'"
            ),
        )
        assert search is not None
        dict_search = search.dict()
        assert "filter-lang" in dict_search
        dict_search["filter-lang"] == "cql2-text"
        assert "filter" in dict_search
        dict_search["filter"] == (
            "filter=id='LC08_L1TP_060247_20180905_20180912_01_T1_L1TP'"
            "AND collection='landsat8_l1tp'"
        )

        search = Search(
            search_type="api", fields=Field(field_type="exclude", fields=["datetime"])
        )
        assert search is not None
        dict_search = search.dict()
        assert "fields" in dict_search
        dict_search["fields"] == {"exclude": ["datetime"]}

        search = Search(
            search_type="api", fields=Field(field_type="include", fields=["datetime"])
        )
        assert search is not None
        dict_search = search.dict()
        assert "fields" in dict_search
        dict_search["fields"] == {"include": ["datetime"]}

        search = Search(
            search_type="api",
            fields=(
                Field(field_type="include", fields=["bbox"]),
                Field(field_type="exclude", fields=["datetime"]),
            ),
        )
        assert search is not None
        dict_search = search.dict()
        assert "fields" in dict_search
        dict_search["fields"] == {"include": ["bbox"], "exclude": ["datetime"]}

    def test_get_request(self):
        search = Search(
            search_type="api",
        )
        assert search is not None
        assert search.get_request() == {"limit": DEFAUL_LIMIT}

        search = Search(
            search_type="api",
            bbox=[-180, -90, 180, 90],
            ids=[1, 2, 3],
            datetime=(
                datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2019, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            ),
            collections=["collection1", "collection2"],
            filter=(
                "filter=id='LC08_L1TP_060247_20180905_20180912_01_T1_L1TP'"
                "AND collection='landsat8_l1tp'"
            ),
            query=[
                Query(property="datetime", operator=[("eq", 2)]),
                Query(property="bbox", operator=[("eq", 4)]),
            ],
            sort_by=[
                SortBy(field="datetime", direction="asc"),
                SortBy(field="bbox", direction="desc"),
            ],
        )
        assert search is not None
        get = search.get_request()
        assert "search_type" not in get
        assert "bbox" in get
        get["bbox"] == "-180,-90,180,90"
        assert "ids" in get
        get["ids"] == "1,2,3"
        assert "datetime" in get
        get["datetime"] == "2018-01-01T00:00:00Z/2019-01-01T00:00:00Z"
        assert "collections" in get
        get["collections"] == "collection1,collection2"
        assert "filter-lang" not in get
        assert "filter" in get
        get["filter"] == (
            "filter=id='LC08_L1TP_060247_20180905_20180912_01_T1_L1TP'"
            "AND collection='landsat8_l1tp'"
        )
        assert "query" in get
        get["query"] == {"datetime": {"eq": 2}, "bbox": {"eq": 4}}
        assert "sortby" in get
        get["sortby"] == "+datetime,-datetime"
        assert "limit" in get
        get["limit"] == DEFAUL_LIMIT

        intersect = {
            "type": "Polygon",
            "coordinates": [
                [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
            ],
        }
        search = Search(search_type="api", intersects=intersect)
        assert search is not None
        get = search.get_request()
        assert "intersects" in get
        get["intersects"] == intersect

        search = Search(
            search_type="api", fields=Field(field_type="exclude", fields=["datetime"])
        )
        assert search is not None
        get = search.get_request()
        assert "fields" in get
        get["fields"] == "-datetime"

        search = Search(
            search_type="api", fields=Field(field_type="include", fields=["datetime"])
        )
        assert search is not None
        get = search.get_request()
        assert "fields" in get
        get["fields"] == "+datetime"

        search = Search(
            search_type="api",
            fields=(
                Field(field_type="include", fields=["bbox"]),
                Field(field_type="exclude", fields=["datetime"]),
            ),
        )
        assert search is not None
        get = search.get_request()
        assert "fields" in get
        get["fields"] == "+bbox,-datetime"

        search = Search(
            search_type="api",
            filter=Filter(
                op="and", args=[{"op": "=", "args": ["datetime", "2020-01-01"]}]
            ),
        )
        assert search is not None
        get = search.get_request()
        assert "search_type" not in get
        assert "filter" not in get
        assert "filter-lang" not in get

    def test_post_request(self):
        search = Search(
            search_type="api",
            bbox=[-180, -90, 180, 90],
            datetime=(
                datetime(2018, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2019, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            ),
            ids=[1, 2, 3],
            collections=["collection1", "collection2"],
            query=[
                Query(property="datetime", operator=[("eq", 2)]),
                Query(property="bbox", operator=[("eq", 4)]),
            ],
            filter=Filter(
                op="and", args=[{"op": "=", "args": ["datetime", "2020-01-01"]}]
            ),
            sort_by=[
                SortBy(field="datetime", direction="asc"),
                SortBy(field="bbox", direction="desc"),
            ],
            fields=(
                Field(field_type="include", fields=["datetime"]),
                Field(field_type="exclude", fields=["bbox"]),
            ),
        )
        assert search is not None
        post = search.post_request()
        assert "search_type" not in post
        assert "bbox" in post
        post["bbox"] == [-180, -90, 180, 90]
        assert "datetime" in post
        post["datetime"] == "2018-01-01T00:00:00Z/2019-01-01T00:00:00Z"
        assert "ids" in post
        post["ids"] == [1, 2, 3]
        assert "collections" in post
        post["collections"] == ["collection1", "collection2"]
        assert "query" in post
        post["query"] == {"datetime": {"eq": 2}, "bbox": {"eq": 4}}
        assert "filter-lang" in post
        post["filter-lang"] == "cql2-json"
        assert "filter" in post
        post["filter"] == {
            "op": "and",
            "args": [{"op": "=", "args": ["datetime", "2020-01-01"]}],
        }
        assert "sortby" in post
        post["sortby"] == [
            {"field": "datetime", "direction": "asc"},
            {"field": "bbox", "direction": "desc"},
        ]
        assert "fields" in post
        post["fields"] == {"include": ["datetime"], "exclude": ["bbox"]}
        assert "limit" in post
        post["limit"] == DEFAUL_LIMIT

        search = Search(
            search_type="api",
            filter=(
                "filter=id='LC08_L1TP_060247_20180905_20180912_01_T1_L1TP'"
                "AND collection='landsat8_l1tp'"
            ),
        )
        assert search is not None
        post = search.post_request()
        assert "search_type" not in post
        assert "filter" not in post
        assert "filter-lang" not in post
