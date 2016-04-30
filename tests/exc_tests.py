import unittest

from zoonado import exc


class ExceptionTests(unittest.TestCase):

    def test_connect_error_string(self):
        e = exc.ConnectError("broker01", 9091, server_id=8)

        self.assertEqual(str(e), "Error connecting to broker01:9091")

    def test_response_error_string(self):
        e = exc.DataInconsistency()

        self.assertEqual(str(e), "DataInconsistency")

    def test_unknown_error_string(self):
        e = exc.UnknownError(-1000)

        self.assertEqual(str(e), "Unknown error code: -1000")

    def test_get_response_error(self):
        e = exc.get_response_error(-8)

        self.assertIsInstance(e, exc.BadArguments)

    def test_get_response_error_unknown(self):
        e = exc.get_response_error(-999)

        self.assertIsInstance(e, exc.UnknownError)

        self.assertEqual(e.error_code, -999)
