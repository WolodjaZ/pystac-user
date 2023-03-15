import pytest

from pystac_user.schema import _OPERATOR_MAP, _QUERY_OPERATOR, Query


class TestQuery:
    """
    The schema looks like this:
        {
            <property>: {
                {<operator1>}: <value1>
                {<operator2>}: <value2>
        }
    Need to have versions:
        - str
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
