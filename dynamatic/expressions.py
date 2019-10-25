from __future__ import annotations
from typing import Any, Union, List
import collections


def ref(token: Any) -> str:
    """Wraps the token so it can be used as an attribute token"""
    return f"#ref{token}"


def val(token: Any) -> str:
    """Wraps the token so it can be used a value token"""
    return f":val{token}"


def serialize(updates: Union[UpdateExpression, List[UpdateExpression]]) -> dict:
    """
    Takes a list of UpdateExpressions (or just a single UpdateExpression) and
    compiles it into a single dict containing the necessary keys to send to 
    DynamoDB
    """
    # If a single expression is passed we wrap it in a list
    if isinstance(updates, UpdateExpression):
        updates = [updates]

    expressions = collections.defaultdict(list)
    response = {
        "UpdateExpression": "",
        "ExpressionAttributeNames": {},
        "ExpressionAttributeValues": {},
    }

    token = 1
    for update in updates:
        expressions[update.action].append(update.expression(token))
        response["ExpressionAttributeNames"].update(update.attribute_names(token))
        response["ExpressionAttributeValues"].update(update.attribute_values(token))
        token += 1

    # Compile the expressions into a string grouped by action
    for action, expressions in expressions.items():
        response["UpdateExpression"] += f"{action} {', '.join(expressions)} "

    # Return response, stripping out empty values
    return {k: v for k, v in response.items() if v}


class UpdateExpression:
    action = "SET"
    path: str
    value = None

    def expression(self, token: Any) -> str:
        return f"{ref(token)} = {val(token)}"

    def attribute_names(self, token: Any) -> dict:
        return {ref(token): self.path}

    def attribute_values(self, token: Any) -> dict:
        return {val(token): self.value}


class Set(UpdateExpression):
    """Sets the value of an attribute"""

    if_not_exists: str = None

    def __init__(self, path: str, value: Any, if_not_exists: str = None):
        self.path = path
        self.value = value
        self.if_not_exists = if_not_exists

    def expression(self, token: Any) -> str:
        if self.if_not_exists:
            return f"{ref(token)} = if_not_exists({ref(token)}b, {val(token)})"
        return super().expression(token)

    def attribute_names(self, token: Any) -> dict:
        if self.if_not_exists:
            return {ref(token): self.path, ref(token) + "b": self.if_not_exists}
        return super().attribute_names(token)


class Increase(Set):
    """Increases the value of an attribute"""

    def expression(self, token: Any):
        if self.if_not_exists:
            return f"{ref(token)} = if_not_exists({ref(token)}b, {ref(token)} + {val(token)})"
        return f"{ref(token)} = {ref(token)} + {val(token)}"


class Decrease(Set):
    """Decreases the value of an attribute"""

    def expression(self, token: Any):
        if self.if_not_exists:
            return f"{ref(token)} = if_not_exists({ref(token)}b, {ref(token)} - {val(token)})"
        return f"{ref(token)} = {ref(token)} - {val(token)}"


class Append(Set):
    """Adds items to the end of a list"""

    def expression(self, token: Any):
        if self.if_not_exists:
            return f"{ref(token)} = if_not_exists({ref(token)}b, list_append({ref(token)}, {val(token)}))"
        return f"{ref(token)} = list_append({ref(token)}, {val(token)})"


class Prepend(Set):
    """Adds items to a list"""

    def expression(self, token: Any):
        if self.if_not_exists:
            return f"{ref(token)} = if_not_exists({ref(token)}b, list_append({val(token)}, {ref(token)}))"
        return f"{ref(token)} = list_append({val(token)}, {ref(token)})"


class Remove(UpdateExpression):
    """Deletes an attribute from an Item"""

    action = "REMOVE"

    def __init__(self, path: str):
        self.path = path

    def expression(self, token: Any):
        return ref(token)

    def attribute_values(self, token: Any):
        return {}


class Add(UpdateExpression):
    """Adds a value to a number or set"""

    action = "ADD"

    def __init__(self, path: str, value: Any):
        self.path = path
        self.value = value

    def expression(self, token: Any):
        return f"{ref(token)} {val(token)}"


class Delete(UpdateExpression):
    """Deletes items of a set from a set"""

    action = "DELETE"

    def __init__(self, path: str, value: set):
        self.path = path
        self.value = value

    def expression(self, token: Any):
        return f"{ref(token)} {val(token)}"
