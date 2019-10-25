from boto3.dynamodb.conditions import Key, Attr, ConditionBase

from .core import (
    BaseTable,
    KeyDefinition,
    Stream,
    SSESpecification,
    ProvisionedThroughput,
)
from .indexes import LocalSecondaryIndex, GlobalSecondaryIndex
from .table_mixins import (
    CreateMixin,
    GetMixin,
    QueryMixin,
    ScanMixin,
    PutMixin,
    DeleteMixin,
    UpdateMixin,
)


class Table(
    CreateMixin,
    GetMixin,
    QueryMixin,
    ScanMixin,
    PutMixin,
    DeleteMixin,
    UpdateMixin,
    BaseTable,
):
    pass
