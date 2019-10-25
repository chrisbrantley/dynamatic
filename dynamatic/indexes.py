from __future__ import annotations
from typing import List, Sequence

from .core import KeyDefinition, ProvisionedThroughput
from .enums import PROJECTION


class BaseSecondaryIndex:
    PROJECTION = PROJECTION

    name: str = None
    _table = None

    def export_projection(self, attributes: Sequence[str] = None):
        if not attributes:
            return {"ProjectionType": "KEYS_ONLY"}
        elif attributes == ["*"]:
            return {"ProjectionType": "ALL"}
        else:
            attributes = list(attributes)
            return {"ProjectionType": "INCLUDE", "NonKeyAttributes": attributes}

    def query(
        self,
        key_condition,
        filter_expression=None,
        attributes: Sequence(str) = None,
        limit: int = None,
        consistent_read: bool = False,
        scan_index_forward: bool = True,
        exclusive_start_key: dict = None,
    ) -> (List[dict], dict):
        return self._table.query(
            key_condition=key_condition,
            filter_expression=filter_expression,
            attributes=attributes,
            limit=limit,
            consistent_read=consistent_read,
            scan_index_forward=scan_index_forward,
            exclusive_start_key=exclusive_start_key,
            _index=self.name,
        )

    def scan(
        self,
        filter_expression=None,
        attributes: Sequence(str) = None,
        limit: int = None,
        consistent_read: bool = False,
        total_segments: int = None,
        segment: int = None,
        exclusive_start_key: dict = None,
    ) -> (List[dict], dict):
        return self._table.scan(
            filter_expression=filter_expression,
            attributes=attributes,
            limit=limit,
            consistent_read=consistent_read,
            total_segments=total_segments,
            segment=segment,
            exclusive_start_key=exclusive_start_key,
            _index=self.name,
        )


class LocalSecondaryIndex(BaseSecondaryIndex):
    name: str = None
    sort_key: KeyDefinition = None
    attributes: Sequence[str] = PROJECTION.ALL
    _table = None

    def __init__(
        self,
        name: str = None,
        sort_key: KeyDefinition = None,
        attributes: Sequence[str] = None,
    ):
        self.name = name or self.name
        self.sort_key = sort_key or self.sort_key
        self.attributes = attributes or self.attributes

    def get_all_keys(self) -> set:
        return {self.sort_key}

    def export(self) -> dict:
        return {
            "IndexName": self.name,
            "KeySchema": KeyDefinition.schema(self._table.partition_key, self.sort_key),
            "Projection": self.export_projection(self.attributes),
        }


class GlobalSecondaryIndex(BaseSecondaryIndex):
    name: str = None
    partition_key: KeyDefinition = None
    sort_key: KeyDefinition = None
    attributes: Sequence[str] = PROJECTION.ALL
    throughput: ProvisionedThroughput = ProvisionedThroughput(5, 5)
    _table = None

    def __init__(
        self,
        name: str = None,
        partition_key: KeyDefinition = None,
        sort_key: KeyDefinition = None,
        attributes: Sequence[str] = None,
        throughput: ProvisionedThroughput = None,
    ):
        self.name = name or self.name
        self.partition_key = partition_key or self.partition_key
        self.sort_key = sort_key or self.sort_key
        self.attributes = attributes or self.attributes
        self.throughput = throughput or self.throughput

    def get_all_keys(self) -> set:
        keys = {self.partition_key, self.sort_key}
        return {k for k in keys if k}

    def export(self) -> dict:
        spec = {
            "IndexName": self.name,
            "KeySchema": KeyDefinition.schema(self.partition_key, self.sort_key),
            "Projection": self.export_projection(self.attributes),
        }
        if self.throughput:
            spec["ProvisionedThroughput"] = self.throughput.export()
        return spec
