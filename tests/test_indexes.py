import unittest

import boto3

from dynamatic import (
    Table,
    KeyDefinition,
    LocalSecondaryIndex,
    GlobalSecondaryIndex,
    Key,
)
from dynamatic.indexes import BaseSecondaryIndex

from dynamatic.exceptions import ResourceNotFoundException

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url="http://localhost:8181",
    aws_access_key_id="AccessKey",
    aws_secret_access_key="VerySecretKey",
    region_name="us-west-2",
)


class MyTable(Table):
    name = "MyTable"
    partition_key = KeyDefinition("pk")
    sort_key = KeyDefinition("sk")
    billing_mode = Table.BILLING_MODE.PAY_PER_REQUEST

    lsi = LocalSecondaryIndex(sort_key=KeyDefinition("status"))
    gsi = GlobalSecondaryIndex(
        partition_key=KeyDefinition("sk"),
        sort_key=KeyDefinition("sequence", KeyDefinition.DATATYPE.NUMBER),
    )


class BaseSecondaryIndexTestCase(unittest.TestCase):
    def test_export_projection(self):
        index = BaseSecondaryIndex()
        assert index.export_projection() == {"ProjectionType": "KEYS_ONLY"}
        assert index.export_projection(["*"]) == {"ProjectionType": "ALL"}
        assert index.export_projection(["foo", "bar"]) == {
            "ProjectionType": "INCLUDE",
            "NonKeyAttributes": ["foo", "bar"],
        }


class IndexTestCase(unittest.TestCase):
    def setUp(self):
        self.table = MyTable(resource=dynamodb)
        try:
            self.table.delete_table()
        except ResourceNotFoundException:
            pass
        self.table.create_table()
        self.table.put({"pk": "1", "sk": "1", "status": "active", "sequence": 1})
        self.table.put({"pk": "1", "sk": "2", "sequence": 2})
        self.table.put({"pk": "1", "sk": "3", "status": "active", "sequence": 3})
        self.table.put({"pk": "2", "sk": "1", "sequence": 4})
        self.table.put({"pk": "2", "sk": "2", "status": "deleted", "sequence": 5})
        self.table.put({"pk": "2", "sk": "3", "sequence": 6})

    def test_lsi_query(self):
        items, _ = self.table.lsi.query(Key("pk").eq("1"))
        assert len(items) == 2  # Only 2 items have the status attribute

    def test_lsi_scan(self):
        items, _ = self.table.lsi.scan()
        assert len(items) == 3

    def test_gsi_query(self):
        items, _ = self.table.gsi.query(Key("sk").eq("3") & Key("sequence").gt(3))
        assert len(items) == 1

    def test_gsi_scan(self):
        items, _ = self.table.gsi.scan()
        assert len(items) == 6
