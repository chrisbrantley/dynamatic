from enum import Enum


class BILLING_MODE(str, Enum):
    PROVISIONED = "PROVISIONED"
    PAY_PER_REQUEST = "PAY_PER_REQUEST"


class RETURN_VALUES(str, Enum):
    NONE = "NONE"
    ALL_OLD = "ALL_OLD"
    UPDATED_OLD = "UPDATED_OLD"
    ALL_NEW = "ALL_NEW"
    UPDATED_NEW = "UPDATED_NEW"


class PROJECTION:
    ALL = ["*"]


class DATATYPE(str, Enum):
    STRING = "S"
    NUMBER = "N"
    BINARY = "B"
    BOOLEAN = "BOOL"
    NUMBER_SET = "NS"
    STRING_SET = "SS"
    BINARY_SET = "BS"
    LIST = "L"
    MAP = "M"


class STREAM_VIEW(str, Enum):
    NEW_IMAGE = "NEW_IMAGE"
    OLD_IMAGE = "OLD_IMAGE"
    NEW_AND_OLD_IMAGES = "NEW_AND_OLD_IMAGES"
    KEYS_ONLY = "KEYS_ONLY"


class SSE_TYPE(str, Enum):
    AES256 = "AES256"
    KMS = "KMS"
