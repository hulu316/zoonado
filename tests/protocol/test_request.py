import unittest
from mock import patch

from zoonado.protocol import request, primitives


class RequestTests(unittest.TestCase):

    @patch.object(request, "struct")
    def test_serialize_without_xid_or_opcode(self, mock_struct):
        mock_struct.pack.return_value = b"fake result"

        class FakeRequest(request.Request):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
            )

        r = FakeRequest(first=3, second=u"foobar")

        result = r.serialize()

        self.assertEqual(result, b"fake result")

        mock_struct.pack.assert_called_once_with("!ii6s", 3, 6, b'foobar')

    @patch.object(request, "struct")
    def test_serialize_with_xid(self, mock_struct):
        mock_struct.pack.return_value = b"fake result"

        class FakeRequest(request.Request):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
            )

        r = FakeRequest(first=3, second=u"foobar")

        result = r.serialize(xid=12)

        self.assertEqual(result, b"fake result")

        mock_struct.pack.assert_called_once_with("!iii6s", 12, 3, 6, b'foobar')

    @patch.object(request, "struct")
    def test_serialize_with_xid_and_opcode(self, mock_struct):
        mock_struct.pack.return_value = b"fake result"

        class FakeRequest(request.Request):
            opcode = 99

            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
            )

        r = FakeRequest(first=3, second=u"foobar")

        result = r.serialize(xid=12)

        self.assertEqual(result, b"fake result")

        mock_struct.pack.assert_called_once_with(
            "!iiii6s", 12, 99, 3, 6, b'foobar'
        )
