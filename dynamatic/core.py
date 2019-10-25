from __future__ import annotations
from typing import Dict, Union, Any, Sequence
import collections

import boto3

from .enums import BILLING_MODE, RETURN_VALUES, DATATYPE, STREAM_VIEW, SSE_TYPE

dynamodb = boto3.resource("dynamodb")


class KeyDefinition:
    DATATYPE = DATATYPE

    name: str
    datatype: DATATYPE

    def __init__(self, name: str, datatype: DATATYPE = DATATYPE.STRING):
        self.name = name
        self.datatype = datatype

    def __eq__(self, other):
        if not isinstance(other, KeyDefinition):
            return False
        return self.name == other.name and self.datatype == other.datatype

    def __hash__(self):
        return hash(self.name + self.datatype)

    def export(self) -> dict:
        return {"AttributeName": self.name, "AttributeType": self.datatype}

    @classmethod
    def schema(cls, hash_key: KeyDefinition, range_key: KeyDefinition = None) -> list:
        schema = [{"AttributeName": hash_key.name, "KeyType": "HASH"}]
        if range_key:
            schema.append({"AttributeName": range_key.name, "KeyType": "RANGE"})
        return schema


class Stream:
    VIEW = STREAM_VIEW

    def __init__(self, view_type: VIEW):
        self.view_type = view_type

    def export(self) -> dict:
        return {"StreamEnabled": True, "StreamViewType": self.view_type}


class SSESpecification:
    SSE_TYPE = SSE_TYPE

    def __init__(self, sse_type: SSE_TYPE, kms_master_key: str = None):
        self.sse_type = sse_type
        self.kms_master_key = kms_master_key

    def export(self) -> dict:
        spec = {"Enabled": True, "SSEType": self.sse_type}
        if self.sse_type == SSE_TYPE.KMS:
            spec["KMSMasterKeyId"] = self.kms_master_key
        return spec


class ProvisionedThroughput:
    def __init__(self, read_capacity: int, write_capacity: int):
        self.read_capacity = read_capacity
        self.write_capacity = write_capacity

    def export(self) -> dict:
        return {
            "ReadCapacityUnits": self.read_capacity,
            "WriteCapacityUnits": self.write_capacity,
        }


class BaseTable:
    BILLING_MODE = BILLING_MODE
    RETURN_VALUES = RETURN_VALUES

    resource = dynamodb
    name: str
    partition_key: KeyDefinition = KeyDefinition("pk")
    sort_key: KeyDefinition = None
    billing_mode: BILLING_MODE = BILLING_MODE.PROVISIONED
    throughput: ProvisionedThroughput = ProvisionedThroughput(5, 5)
    stream: Stream = None
    sse: SSESpecification = None
    tags: Dict = {}

    def __init__(self, **kwargs):
        self._local_secondary_indexes = []
        self._global_secondary_indexes = []
        self.name = kwargs.get("name") or self.name
        self.billing_mode = kwargs.get("billing_mode") or self.billing_mode
        self.throughput = kwargs.get("throughput") or self.throughput
        self.stream = kwargs.get("stream") or self.stream
        self.sse = kwargs.get("sse") or self.sse
        self.resource = kwargs.get("resource") or self.resource
        if kwargs.get("tags"):
            self.tags.update(kwargs["tags"])

    def get_table(self):
        return self.resource.Table(self.name)

    def convert_key(self, key: Union[Any, Sequence[Any, Any]]) -> dict:
        if isinstance(key, str) or not isinstance(key, collections.abc.Sequence):
            key = (key,)
        converted = {self.partition_key.name: key[0]}
        if self.sort_key and len(key) > 1:
            converted[self.sort_key.name] = key[1]
        return converted

    def serialize_attributes(self, attributes: Sequence[str]) -> dict:
        attribute_names = {}
        attribute_tokens = []
        counter = 0
        for attribute in attributes:
            token = f"#ref{counter}"
            attribute_names[token] = attribute
            attribute_tokens.append(token)
            counter += 1
        return {
            "ProjectionExpression": ", ".join(attribute_tokens),
            "ExpressionAttributeNames": attribute_names,
        }
