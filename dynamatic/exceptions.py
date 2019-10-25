from botocore.exceptions import ClientError


def handle_client_error(client_error: ClientError):

    exception_map = {
        "ConditionalCheckFailedException": ConditionalCheckFailedException,
        "ResourceNotFoundException": ResourceNotFoundException,
        "ResourceInUseException": ResourceInUseException,
    }

    try:
        raise exception_map[client_error.response["Error"]["Code"]]()
    except (KeyError, AttributeError):
        raise client_error


class DynamaticError(Exception):
    pass


class ConditionalCheckFailedException(DynamaticError):
    pass


class ResourceNotFoundException(DynamaticError):
    pass


class ResourceInUseException(DynamaticError):
    pass


class ItemNotFoundException(DynamaticError):
    pass
