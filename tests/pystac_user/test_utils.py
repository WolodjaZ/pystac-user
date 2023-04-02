from pystac_user.utils import merge_schemas_dict


def test_merge_schemas_dict():
    class SimpleDictObject:
        def __init__(self, obj):
            self.obj = obj

        def dict(self):
            return self.obj

    one_element = {"obj": 1, "obj1": {"obj2": 2, "obj3": 3}}
    one_obj = SimpleDictObject(one_element)
    return_element = merge_schemas_dict([one_obj])
    assert return_element == one_element

    two_elements = [
        {"obj": 1, "obj1": {"obj2": 2, "obj3": 3}},
        {"obj4": 4, "obj5": {"obj6": 5, "obj7": 6}},
    ]
    two_obj = [SimpleDictObject(obj) for obj in two_elements]
    return_element = merge_schemas_dict(two_obj)
    assert return_element == {
        "obj": 1,
        "obj1": {"obj2": 2, "obj3": 3},
        "obj4": 4,
        "obj5": {"obj6": 5, "obj7": 6},
    }

    three_elemnts = [
        {"obj": 1, "obj1": {"obj2": 2, "obj3": 3}},
        {"obj4": 4, "obj5": {"obj6": 5, "obj7": 6}},
        {"obj8": 7, "obj9": {"obj10": 8, "obj11": 9}},
    ]
    three_obj = [SimpleDictObject(obj) for obj in three_elemnts]
    return_element = merge_schemas_dict(three_obj)
    assert return_element == {
        "obj": 1,
        "obj1": {"obj2": 2, "obj3": 3},
        "obj4": 4,
        "obj5": {"obj6": 5, "obj7": 6},
        "obj8": 7,
        "obj9": {"obj10": 8, "obj11": 9},
    }
