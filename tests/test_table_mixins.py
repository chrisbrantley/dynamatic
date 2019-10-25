import unittest

import boto3

from dynamatic import (
    Table,
    KeyDefinition,
    GlobalSecondaryIndex,
    LocalSecondaryIndex,
    Key,
    Attr,
    Stream,
    SSESpecification,
    ProvisionedThroughput,
)
from dynamatic.exceptions import (
    ConditionalCheckFailedException,
    ResourceNotFoundException,
    ResourceInUseException,
    ItemNotFoundException,
)
from dynamatic.expressions import (
    Set,
    Increase,
    Decrease,
    Append,
    Prepend,
    Remove,
    Add,
    Delete,
)

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
    billing_mode = Table.BILLING_MODE.PROVISIONED
    throughput = ProvisionedThroughput(100, 150)

    lsi = LocalSecondaryIndex(sort_key=KeyDefinition("status"))
    gsi = GlobalSecondaryIndex(
        partition_key=KeyDefinition("sk"),
        sort_key=KeyDefinition("sequence", KeyDefinition.DATATYPE.NUMBER),
    )

    stream = Stream(Stream.VIEW.NEW_AND_OLD_IMAGES)
    sse = SSESpecification(SSESpecification.SSE_TYPE.KMS, "12345")

    tags = {"foo": "bar"}


class CreateMixinTestCase(unittest.TestCase):
    def test_init(self):
        # pylint: disable=protected-access
        table = MyTable(resource=dynamodb)
        assert len(table._local_secondary_indexes) == 1
        assert isinstance(table._local_secondary_indexes[0], LocalSecondaryIndex)
        assert len(table._global_secondary_indexes) == 1
        assert isinstance(table._global_secondary_indexes[0], GlobalSecondaryIndex)

    def test_get_all_keys(self):
        table = MyTable(resource=dynamodb)
        keys = table.get_all_keys()
        assert keys == {
            KeyDefinition("pk"),
            KeyDefinition("sk"),
            KeyDefinition("status"),
            KeyDefinition("sequence", KeyDefinition.DATATYPE.NUMBER),
        }

    def test_export(self):
        table = MyTable(resource=dynamodb)
        exported = table.export()
        assert exported == {
            "TableName": "MyTable",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sequence", "AttributeType": "N"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "BillingMode": "PROVISIONED",
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 100,
                "WriteCapacityUnits": 150,
            },
            "StreamSpecification": {
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
            "SSESpecification": {
                "Enabled": True,
                "SSEType": "KMS",
                "KMSMasterKeyId": "12345",
            },
            "Tags": [{"Key": "foo", "Value": "bar"}],
            "LocalSecondaryIndexes": [
                {
                    "IndexName": "lsi",
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "status", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "gsi",
                    "KeySchema": [
                        {"AttributeName": "sk", "KeyType": "HASH"},
                        {"AttributeName": "sequence", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5,
                    },
                }
            ],
        }

    def test_create__delete_table(self):
        table = MyTable(resource=dynamodb)

        # Let's attempt to delete the table in case it exists
        try:
            table.delete_table()
        except ResourceNotFoundException:
            pass

        table.create_table()

        # If we try to create the table again we should get a ResourceInUseException
        with self.assertRaises(ResourceInUseException):
            table.create_table()

        # Now let's delete it
        table.delete_table()

        # If we try to deletethe table again we should get a ResourceNotFoundException
        with self.assertRaises(ResourceNotFoundException):
            table.delete_table()


class GetMixinTestCase(unittest.TestCase):
    def setUp(self):
        self.table = MyTable(resource=dynamodb)
        try:
            self.table.delete_table()
        except ResourceNotFoundException:
            pass
        self.table.create_table()

    def test_get(self):
        self.table.put(
            {"pk": "Partition1", "sk": "Sort1", "status": "active", "sequence": 1}
        )
        item = self.table.get(("Partition1", "Sort1"))
        assert item["status"] == "active"

    def test_get_attributes(self):
        self.table.put(
            {"pk": "Partition1", "sk": "Sort1", "status": "active", "sequence": 1}
        )
        item = self.table.get(("Partition1", "Sort1"), attributes=["status", "sk"])
        assert item == {"status": "active", "sk": "Sort1"}

    def test_get_does_not_exist(self):
        with self.assertRaises(ItemNotFoundException):
            self.table.get(("Foo", "Bar"))

    def test_get_client_error(self):
        table = MyTable(name="ThisTableDoesntExist", resource=dynamodb)
        with self.assertRaises(ResourceNotFoundException):
            table.get(("Partition1", "Sort1"), attributes=["status", "sk"])


class QueryMixinTestCase(unittest.TestCase):
    def setUp(self):
        self.table = MyTable(resource=dynamodb)
        try:
            self.table.delete_table()
        except ResourceNotFoundException:
            pass
        self.table.create_table()
        self.table.put({"pk": "1", "sk": "1", "status": "active", "sequence": 1})
        self.table.put({"pk": "1", "sk": "2", "status": "active", "sequence": 2})
        self.table.put({"pk": "1", "sk": "3", "status": "active", "sequence": 3})
        self.table.put({"pk": "2", "sk": "1", "status": "active", "sequence": 4})
        self.table.put({"pk": "2", "sk": "2", "status": "deleted", "sequence": 5})
        self.table.put({"pk": "2", "sk": "3", "status": "deleted", "sequence": 6})

    def test_query(self):
        items, _ = self.table.query(
            Key("pk").eq("1"),
            filter_expression=Attr("sequence").gt(1),
            attributes=["sequence"],
        )
        assert len(items) == 2

    def test_query_pagination(self):
        items, last = self.table.query(Key("pk").eq("1"), limit=1)
        assert len(items) == 1
        assert items[0]["sequence"] == 1
        items, last = self.table.query(
            Key("pk").eq("1"), limit=5, exclusive_start_key=last
        )
        assert len(items) == 2
        assert items[0]["sequence"] == 2
        assert last is None

    def test_query_reverse_scan(self):
        items, last = self.table.query(
            Key("pk").eq("1"), limit=3, scan_index_forward=False
        )
        assert len(items) == 3
        assert items[0]["sequence"] == 3
        assert last == {"sk": "1", "pk": "1"}

    def test_query_index(self):
        items, _ = self.table.query(
            Key("pk").eq("2") & Key("status").eq("deleted"), _index="lsi"
        )
        assert len(items) == 2

    def test_query_client_error(self):
        table = MyTable(name="ThisTableDoesntExist", resource=dynamodb)
        with self.assertRaises(ResourceNotFoundException):
            table.query(Key("pk").eq("1"))


class ScanMixinTestCase(unittest.TestCase):
    def setUp(self):
        self.table = MyTable(resource=dynamodb)
        try:
            self.table.delete_table()
        except ResourceNotFoundException:
            pass
        self.table.create_table()
        self.table.put({"pk": "1", "sk": "1", "status": "active", "sequence": 1})
        self.table.put({"pk": "1", "sk": "2", "status": "active", "sequence": 2})
        self.table.put({"pk": "1", "sk": "3", "status": "active", "sequence": 3})
        self.table.put({"pk": "2", "sk": "1", "status": "active", "sequence": 4})
        self.table.put({"pk": "2", "sk": "2", "sequence": 5})
        self.table.put({"pk": "2", "sk": "3", "sequence": 6})

    def test_scan(self):
        items, _ = self.table.scan(
            filter_expression=Attr("status").eq("active"),
            attributes=["sequence", "status"],
            limit=10,
            total_segments=1,
            segment=0,
            exclusive_start_key={"pk": "1", "sk": "1"},  # Skip the first item
        )
        assert len(items) == 3

    def test_scan_index(self):
        items, _ = self.table.scan(_index="lsi")
        assert (
            len(items) == 4
        )  # This is a sparse index on status and only 4 items have status

    def test_scan_client_error(self):
        table = MyTable(name="ThisTableDoesntExist", resource=dynamodb)
        with self.assertRaises(ResourceNotFoundException):
            table.scan()


class PutMixinTestCase(unittest.TestCase):
    def setUp(self):
        self.table = MyTable(resource=dynamodb)
        try:
            self.table.delete_table()
        except ResourceNotFoundException:
            pass
        self.table.create_table()

    def test_put(self):
        item = self.table.put(
            {"pk": "Partition1", "sk": "Sort1", "status": "active", "sequence": 1}
        )
        assert item == {}

    def test_put_with_conditions(self):
        item = self.table.put(
            {"pk": "Partition1", "sk": "Sort1", "status": "active", "sequence": 1},
            condition=Attr("sk").ne("Sort1"),
        )
        assert item == {}

        with self.assertRaises(ConditionalCheckFailedException):
            self.table.put(
                {"pk": "Partition1", "sk": "Sort1", "status": "active", "sequence": 1},
                condition=Attr("sk").ne("Sort1"),
            )

    def test_put_with_return_values(self):
        self.table.put(
            {"pk": "Partition1", "sk": "Sort1", "status": "active", "sequence": 1}
        )
        values = self.table.put(
            {"pk": "Partition1", "sk": "Sort1", "status": "archived", "sequence": 2},
            return_values=MyTable.RETURN_VALUES.ALL_OLD,
        )
        assert values["status"] == "active"
        assert values["sequence"] == 1


class DeleteMixinTestCase(unittest.TestCase):
    def setUp(self):
        self.table = MyTable(resource=dynamodb)
        try:
            self.table.delete_table()
        except ResourceNotFoundException:
            pass
        self.table.create_table()

    def test_delete(self):
        self.table.put(
            {"pk": "Partition1", "sk": "Sort1", "status": "active", "sequence": 1}
        )
        self.table.delete(("Partition1", "Sort1"))
        with self.assertRaises(ItemNotFoundException):
            self.table.get(("Partition1", "Sort1"))

    def test_delete_with_conditions(self):
        self.table.put(
            {"pk": "Partition1", "sk": "Sort1", "status": "active", "sequence": 1}
        )
        self.table.delete(
            ("Partition1", "Sort1"), condition=Attr("status").eq("active")
        )
        with self.assertRaises(ConditionalCheckFailedException):
            self.table.delete(
                ("Partition1", "Sort1"), condition=Attr("status").eq("active")
            )

    def test_delete_with_return_values(self):
        self.table.put(
            {"pk": "Partition1", "sk": "Sort1", "status": "active", "sequence": 1}
        )
        values = self.table.delete(
            ("Partition1", "Sort1"), return_values=Table.RETURN_VALUES.ALL_OLD
        )
        assert values["status"] == "active"
        assert values["sequence"] == 1


class UpdateMixinTestCase(unittest.TestCase):
    def setUp(self):
        self.table = MyTable(resource=dynamodb)
        try:
            self.table.delete_table()
        except ResourceNotFoundException:
            pass
        self.table.create_table()

        self.table.put(
            {
                "pk": "Partition1",
                "sk": "Sort1",
                "status": "active",
                "sequence": 1,
                "tags": {"blue", "red", "green"},
                "states": ["Texas", "California", "Oregon"],
            }
        )

    def test_update_set(self):
        updates = Set("status", "in_progress")
        attributes = self.table.update(("Partition1", "Sort1"), updates)
        assert attributes == {}
        item = self.table.get(("Partition1", "Sort1"))
        assert item["status"] == "in_progress"

    def test_update_set_dict(self):
        updates = {"status": "in_progress", "foo": "bar"}
        attributes = self.table.update(("Partition1", "Sort1"), updates)
        assert attributes == {}
        item = self.table.get(("Partition1", "Sort1"))
        assert item["status"] == "in_progress"
        assert item["foo"] == "bar"

    def test_update_set_if_not_exists(self):
        self.table.update(
            ("Partition1", "Sort1"),
            Set("status", "in_progress", if_not_exists="semiphore"),
        )

        item = self.table.get(("Partition1", "Sort1"))
        assert item["status"] == "in_progress"

        self.table.update(
            ("Partition1", "Sort1"), Set("status", "archived", if_not_exists="status")
        )

        # Update should have failed
        item = self.table.get(("Partition1", "Sort1"))
        assert item["status"] == "in_progress"

    def test_update_increase(self):
        self.table.update(("Partition1", "Sort1"), Increase("sequence", 3))

        item = self.table.get(("Partition1", "Sort1"))
        assert item["sequence"] == 4

    def test_update_decrease(self):
        self.table.update(("Partition1", "Sort1"), Decrease("sequence", 3))

        item = self.table.get(("Partition1", "Sort1"))
        assert item["sequence"] == -2

    def test_update_append(self):
        self.table.update(("Partition1", "Sort1"), Append("states", ["New York"]))

        item = self.table.get(("Partition1", "Sort1"))
        assert item["states"] == ["Texas", "California", "Oregon", "New York"]

    def test_update_prepend(self):
        self.table.update(("Partition1", "Sort1"), Prepend("states", ["New York"]))

        item = self.table.get(("Partition1", "Sort1"))
        assert item["states"] == ["New York", "Texas", "California", "Oregon"]

    def test_update_remove(self):
        self.table.update(("Partition1", "Sort1"), Remove("status"))

        item = self.table.get(("Partition1", "Sort1"))
        assert "status" not in item

    def test_update_add_number(self):
        self.table.update(("Partition1", "Sort1"), Add("sequence", 3))

        item = self.table.get(("Partition1", "Sort1"))
        assert item["sequence"] == 4

    def test_update_add_to_set(self):
        self.table.update(("Partition1", "Sort1"), Add("tags", {"purple"}))

        item = self.table.get(("Partition1", "Sort1"))
        assert item["tags"] == {"blue", "red", "green", "purple"}

        # Should be a no-op because green already exists in the set
        self.table.update(("Partition1", "Sort1"), Add("tags", {"green"}))
        item = self.table.get(("Partition1", "Sort1"))
        assert item["tags"] == {"blue", "red", "green", "purple"}

    def test_update_delete(self):
        self.table.update(("Partition1", "Sort1"), Delete("tags", {"red"}))

        item = self.table.get(("Partition1", "Sort1"))
        assert item["tags"] == {"blue", "green"}

    def test_update_multiple_expressions(self):
        self.table.update(
            ("Partition1", "Sort1"),
            [
                Set("status", "in_progress"),
                Increase("sequence", 10),
                Delete("tags", {"red"}),
            ],
        )
        item = self.table.get(("Partition1", "Sort1"))
        assert item["status"] == "in_progress"
        assert item["sequence"] == 11
        assert item["tags"] == {"blue", "green"}

    def test_update_with_conditions(self):
        self.table.update(
            ("Partition1", "Sort1"),
            Set("status", "in_progress"),
            condition=Attr("status").eq("active")
        )
        
        with self.assertRaises(ConditionalCheckFailedException):
            self.table.update(
                ("Partition1", "Sort1"),
                Set("status", "in_progress"),
                condition=Attr("status").eq("active")
            )
