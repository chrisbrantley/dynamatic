import unittest

from dynamatic.expressions import (
    serialize,
    Set,
    Increase,
    Decrease,
    Append,
    Prepend,
    Remove,
    Add,
    Delete,
)


class SerializeTestCase(unittest.TestCase):
    def test_serialize_multiple_expressions(self):
        response = serialize([Set("foo", "bar"), Set("biz", 5), Add("bar", 2)])
        assert response == {
            "UpdateExpression": "SET #ref1 = :val1, #ref2 = :val2 ADD #ref3 :val3 ",
            "ExpressionAttributeNames": {
                "#ref1": "foo",
                "#ref2": "biz",
                "#ref3": "bar",
            },
            "ExpressionAttributeValues": {":val1": "bar", ":val2": 5, ":val3": 2},
        }


class SetTestCase(unittest.TestCase):
    def test_serialize(self):
        response = serialize(Set("foo", "bar"))
        assert response == {
            "UpdateExpression": "SET #ref1 = :val1 ",
            "ExpressionAttributeNames": {"#ref1": "foo"},
            "ExpressionAttributeValues": {":val1": "bar"},
        }

    def test_serialize_if_not_exists(self):
        response = serialize(Set("foo", "bar", if_not_exists="biz"))
        assert response == {
            "UpdateExpression": "SET #ref1 = if_not_exists(#ref1b, :val1) ",
            "ExpressionAttributeNames": {"#ref1": "foo", "#ref1b": "biz"},
            "ExpressionAttributeValues": {":val1": "bar"},
        }


class IncreaseTestCase(unittest.TestCase):
    def test_serialize(self):
        response = serialize(Increase("foo", 1))
        assert response == {
            "UpdateExpression": "SET #ref1 = #ref1 + :val1 ",
            "ExpressionAttributeNames": {"#ref1": "foo"},
            "ExpressionAttributeValues": {":val1": 1},
        }

    def test_serialize_if_not_exists(self):
        response = serialize(Increase("foo", 1, if_not_exists="biz"))
        assert response == {
            "UpdateExpression": "SET #ref1 = if_not_exists(#ref1b, #ref1 + :val1) ",
            "ExpressionAttributeNames": {"#ref1": "foo", "#ref1b": "biz"},
            "ExpressionAttributeValues": {":val1": 1},
        }


class DecreaseTestCase(unittest.TestCase):
    def test_serialize(self):
        response = serialize(Decrease("foo", 1))
        assert response == {
            "UpdateExpression": "SET #ref1 = #ref1 - :val1 ",
            "ExpressionAttributeNames": {"#ref1": "foo"},
            "ExpressionAttributeValues": {":val1": 1},
        }

    def test_serialize_if_not_exists(self):
        response = serialize(Decrease("foo", 1, if_not_exists="biz"))
        assert response == {
            "UpdateExpression": "SET #ref1 = if_not_exists(#ref1b, #ref1 - :val1) ",
            "ExpressionAttributeNames": {"#ref1": "foo", "#ref1b": "biz"},
            "ExpressionAttributeValues": {":val1": 1},
        }


class AppendTestCase(unittest.TestCase):
    def test_serialize(self):
        response = serialize(Append("foo", 1))
        assert response == {
            "UpdateExpression": "SET #ref1 = list_append(#ref1, :val1) ",
            "ExpressionAttributeNames": {"#ref1": "foo"},
            "ExpressionAttributeValues": {":val1": 1},
        }

    def test_serialize_if_not_exists(self):
        response = serialize(Append("foo", 1, if_not_exists="biz"))
        assert response == {
            "UpdateExpression": "SET #ref1 = if_not_exists(#ref1b, list_append(#ref1, :val1)) ",
            "ExpressionAttributeNames": {"#ref1": "foo", "#ref1b": "biz"},
            "ExpressionAttributeValues": {":val1": 1},
        }


class PrependTestCase(unittest.TestCase):
    def test_serialize(self):
        response = serialize(Prepend("foo", 1))
        assert response == {
            "UpdateExpression": "SET #ref1 = list_append(:val1, #ref1) ",
            "ExpressionAttributeNames": {"#ref1": "foo"},
            "ExpressionAttributeValues": {":val1": 1},
        }

    def test_serialize_if_not_exists(self):
        response = serialize(Prepend("foo", 1, if_not_exists="biz"))
        assert response == {
            "UpdateExpression": "SET #ref1 = if_not_exists(#ref1b, list_append(:val1, #ref1)) ",
            "ExpressionAttributeNames": {"#ref1": "foo", "#ref1b": "biz"},
            "ExpressionAttributeValues": {":val1": 1},
        }


class RemoveTestCase(unittest.TestCase):
    def test_serialize(self):
        response = serialize(Remove("foo"))
        assert response == {
            "UpdateExpression": "REMOVE #ref1 ",
            "ExpressionAttributeNames": {"#ref1": "foo"},
        }


class AddTestCase(unittest.TestCase):
    def test_serialize(self):
        response = serialize(Add("foo", 1))
        assert response == {
            "UpdateExpression": "ADD #ref1 :val1 ",
            "ExpressionAttributeNames": {"#ref1": "foo"},
            "ExpressionAttributeValues": {":val1": 1},
        }


class DeleteTestCase(unittest.TestCase):
    def test_serialize(self):
        response = serialize(Delete("foo", 1))
        assert response == {
            "UpdateExpression": "DELETE #ref1 :val1 ",
            "ExpressionAttributeNames": {"#ref1": "foo"},
            "ExpressionAttributeValues": {":val1": 1},
        }

