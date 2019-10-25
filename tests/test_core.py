import unittest

from dynamatic.core import (
    KeyDefinition,
    Stream,
    SSESpecification,
    ProvisionedThroughput,
    BaseTable,
)


class KeyDefinitionTestCase(unittest.TestCase):
    def test_eq(self):
        assert KeyDefinition("my_key") == KeyDefinition("my_key")
        assert KeyDefinition("my_key") != KeyDefinition("foo")
        assert KeyDefinition("my_key") != KeyDefinition(
            "my_key", KeyDefinition.DATATYPE.NUMBER
        )
        assert KeyDefinition("my_key") != "foobar"

    def test_hash(self):
        keys = [
            KeyDefinition("foo", KeyDefinition.DATATYPE.STRING),
            KeyDefinition("foo", KeyDefinition.DATATYPE.NUMBER),  # Different type
            KeyDefinition("bar"),
            KeyDefinition("bar"),  # Duplicate
            KeyDefinition("biz"),
        ]
        key_set = set(keys)
        assert len(key_set) == 4

    def test_export(self):
        key = KeyDefinition("my_key")
        assert key.export() == {
            "AttributeName": "my_key",
            "AttributeType": key.datatype,
        }

    def test_schema_simple_key(self):
        hash_key = KeyDefinition("pk")
        schema = KeyDefinition.schema(hash_key)
        assert schema == [{"AttributeName": hash_key.name, "KeyType": "HASH"}]

    def test_schema_composite_key(self):
        hash_key = KeyDefinition("pk")
        range_key = KeyDefinition("sk")
        schema = KeyDefinition.schema(hash_key, range_key)
        assert schema == [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ]


class StreamTestCase(unittest.TestCase):
    def test_export(self):
        stream = Stream(Stream.VIEW.NEW_AND_OLD_IMAGES)
        assert stream.export() == {
            "StreamEnabled": True,
            "StreamViewType": Stream.VIEW.NEW_AND_OLD_IMAGES,
        }


class SSESpecificationTestCase(unittest.TestCase):
    def test_export_kms(self):
        sse = SSESpecification(SSESpecification.SSE_TYPE.KMS, "12345")
        assert sse.export() == {
            "Enabled": True,
            "SSEType": SSESpecification.SSE_TYPE.KMS,
            "KMSMasterKeyId": "12345",
        }

    def test_export_AES256(self):
        sse = SSESpecification(SSESpecification.SSE_TYPE.AES256)
        assert sse.export() == {
            "Enabled": True,
            "SSEType": SSESpecification.SSE_TYPE.AES256,
        }


class ProvisionedThroughputTestCase(unittest.TestCase):
    def test_export(self):
        throughput = ProvisionedThroughput(100, 50)
        assert throughput.export() == {
            "ReadCapacityUnits": 100,
            "WriteCapacityUnits": 50,
        }


class BaseTableTestCase(unittest.TestCase):
    def test_init(self):
        table = BaseTable(name="TestTable", tags={"foo": "bar"})
        assert table.name == "TestTable"
        assert table.tags == {"foo": "bar"}

    def test_get_table(self):
        from boto3.dynamodb.table import TableResource

        table = BaseTable(name="TestTable")
        assert isinstance(table.get_table(), TableResource)

    def test_convert_key(self):
        class MyTable(BaseTable):
            name = "MyTable"
            partition_key = KeyDefinition("pk")
            sort_key = KeyDefinition("sk")

        table = MyTable()
        assert table.convert_key("foo") == {"pk": "foo"}
        assert table.convert_key(("foo", "bar")) == {"pk": "foo", "sk": "bar"}

    def test_serialize_attributes(self):
        table = BaseTable(name="TestTable")
        response = table.serialize_attributes(["foo", "bar", "biz"])
        assert response == {
            "ProjectionExpression": "#ref0, #ref1, #ref2",
            "ExpressionAttributeNames": {
                "#ref0": "foo",
                "#ref1": "bar",
                "#ref2": "biz",
            },
        }
