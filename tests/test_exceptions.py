import unittest

from botocore.exceptions import ClientError

from dynamatic.exceptions import handle_client_error, ResourceNotFoundException


class ExceptionsTestCase(unittest.TestCase):
    def test_handle_client_error(self):
        client_error = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, operation_name="test"
        )
        with self.assertRaises(ResourceNotFoundException):
            handle_client_error(client_error)

    def test_handle_client_error_not_implemented(self):
        client_error = ClientError({"Error": {"Code": "FooBar"}}, operation_name="test")
        with self.assertRaises(ClientError):
            handle_client_error(client_error)

        client_error = ClientError({}, operation_name="test")
        with self.assertRaises(ClientError):
            handle_client_error(client_error)

        other_error = ResourceNotFoundException()
        with self.assertRaises(ResourceNotFoundException):
            handle_client_error(other_error)
