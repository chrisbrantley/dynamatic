from __future__ import annotations
from copy import copy
from typing import Sequence, Union, Any, List

import boto3
from boto3.dynamodb.conditions import ConditionBase

from .exceptions import ClientError, ItemNotFoundException, handle_client_error
from .expressions import UpdateExpression, serialize
from .enums import BILLING_MODE, RETURN_VALUES
from .core import KeyDefinition
from .indexes import BaseSecondaryIndex, LocalSecondaryIndex, GlobalSecondaryIndex


dynamodb = boto3.resource("dynamodb")


class CreateMixin:
    _local_secondary_indexes = []
    _global_secondary_indexes = []

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # register secondary indexes
        for (k, v) in self.__class__.__dict__.items():
            if not isinstance(v, BaseSecondaryIndex):
                continue

            index = copy(v)
            index.name = k
            index._table = self
            setattr(self, k, index)
            if isinstance(index, LocalSecondaryIndex):
                self._local_secondary_indexes.append(index)
            if isinstance(index, GlobalSecondaryIndex):
                self._global_secondary_indexes.append(index)

    def get_all_keys(self) -> set:
        keys = [self.partition_key, self.sort_key]
        for index in self._local_secondary_indexes + self._global_secondary_indexes:
            keys += index.get_all_keys()
        return set(keys)

    def export(self) -> dict:
        spec = {
            "TableName": self.name,
            "AttributeDefinitions": [
                key.export()
                for key in sorted(
                    self.get_all_keys(), key=lambda x: x.name
                )  # sorting the keys gives predictable output
                if key is not None
            ],
            "KeySchema": KeyDefinition.schema(self.partition_key, self.sort_key),
            "BillingMode": self.billing_mode,
        }
        if self.billing_mode == BILLING_MODE.PROVISIONED:
            spec["ProvisionedThroughput"] = self.throughput.export()
        if self.stream:
            spec["StreamSpecification"] = self.stream.export()
        if self.sse:
            spec["SSESpecification"] = self.sse.export()
        if self.tags:
            spec["Tags"] = [{"Key": k, "Value": v} for (k, v) in self.tags.items()]
        if self._local_secondary_indexes:
            spec["LocalSecondaryIndexes"] = [
                index.export() for index in self._local_secondary_indexes
            ]
        if self._global_secondary_indexes:
            spec["GlobalSecondaryIndexes"] = [
                index.export() for index in self._global_secondary_indexes
            ]
        return spec

    def create_table(self):
        try:
            self.resource.create_table(**self.export())
        except ClientError as e:
            handle_client_error(e)

    def delete_table(self):
        try:
            self.get_table().delete()
        except ClientError as e:
            handle_client_error(e)


class GetMixin:
    def get(
        self, key: Union[Any, Sequence[Any, Any]], attributes: Sequence[str] = None
    ) -> dict:
        request = {"Key": self.convert_key(key)}
        if attributes:
            request.update(self.serialize_attributes(attributes))
        try:
            response = self.get_table().get_item(**request)
            return response["Item"]
        except KeyError:
            raise ItemNotFoundException()
        except ClientError as e:
            handle_client_error(e)


class QueryMixin:
    def query(
        self,
        key_condition,
        filter_expression=None,
        attributes: Sequence(str) = None,
        limit: int = None,
        consistent_read: bool = False,
        scan_index_forward: bool = True,
        exclusive_start_key: dict = None,
        _index: str = None,
    ) -> (List[dict], dict):
        request = {
            "KeyConditionExpression": key_condition,
            "ConsistentRead": consistent_read,
            "ScanIndexForward": scan_index_forward,
        }
        if filter_expression:
            request["FilterExpression"] = filter_expression
        if attributes:
            request.update(self.serialize_attributes(attributes))
        if limit:
            request["Limit"] = limit
        if exclusive_start_key:
            request["ExclusiveStartKey"] = exclusive_start_key
        if _index:
            request["IndexName"] = _index
        try:
            response = self.get_table().query(**request)
            return (response["Items"], response.get("LastEvaluatedKey"))
        except ClientError as e:
            handle_client_error(e)


class ScanMixin:
    def scan(
        self,
        filter_expression=None,
        attributes: Sequence(str) = None,
        limit: int = None,
        consistent_read: bool = False,
        total_segments: int = None,
        segment: int = None,
        exclusive_start_key: dict = None,
        _index: str = None,
    ) -> (List[dict], dict):
        request = {"ConsistentRead": consistent_read}
        if filter_expression:
            request["FilterExpression"] = filter_expression
        if attributes:
            request.update(self.serialize_attributes(attributes))
        if limit:
            request["Limit"] = limit
        if exclusive_start_key:
            request["ExclusiveStartKey"] = exclusive_start_key
        if total_segments:
            request["TotalSegments"] = total_segments
        if segment is not None:
            request["Segment"] = segment
        if _index:
            request["IndexName"] = _index
        try:
            response = self.get_table().scan(**request)
            return (response["Items"], response.get("LastEvaluatedKey"))
        except ClientError as e:
            handle_client_error(e)


class PutMixin:
    def put(
        self,
        item: dict,
        condition: ConditionBase = None,
        return_values: RETURN_VALUES = RETURN_VALUES.NONE,
    ) -> dict:
        request = {"Item": item}
        if condition:
            request["ConditionExpression"] = condition
        if return_values:
            request["ReturnValues"] = return_values
        try:
            response = self.get_table().put_item(**request)
            return response.get("Attributes", {})
        except ClientError as e:
            handle_client_error(e)


class DeleteMixin:
    def delete(
        self,
        key: Union[Any, Sequence[Any, Any]],
        condition: ConditionBase = None,
        return_values: RETURN_VALUES = RETURN_VALUES.NONE,
    ) -> dict:
        request = {"Key": self.convert_key(key)}
        if condition:
            request["ConditionExpression"] = condition
        if return_values:
            request["ReturnValues"] = return_values
        try:
            response = self.get_table().delete_item(**request)
            return response.get("Attributes", {})
        except ClientError as e:
            handle_client_error(e)


class UpdateMixin:
    def update(
        self,
        key: Union[Any, Sequence[Any, Any]],
        updates: Union[UpdateExpression, List[UpdateExpression]],
        condition: ConditionBase = None,
        return_values: RETURN_VALUES = RETURN_VALUES.NONE,
    ):
        request = {"Key": self.convert_key(key)}
        if condition:
            request["ConditionExpression"] = condition
        if return_values:
            request["ReturnValues"] = return_values
        request.update(serialize(updates))
        try:
            response = self.get_table().update_item(**request)
            return response.get("Attributes", {})
        except ClientError as e:
            handle_client_error(e)

