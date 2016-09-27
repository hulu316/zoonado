import struct
import unittest

from mock import patch

from zoonado import exc
from zoonado.protocol import transaction, request, response, primitives


class FakeOpRequest(request.Request):
    opcode = 99
    parts = (
        ("foo", primitives.UString),
    )


class FakeOpResponse(response.Response):
    opcode = 99
    parts = (
        ("foo", primitives.UString),
    )


class FakeSpecialRequest(request.Request):
    opcode = 999
    special_xid = -99
    parts = ()


class FakeSpecialResponse(response.Response):
    opcode = 999
    parts = ()


fake_response_xref = {
    response_class.opcode: response_class
    for response_class in (FakeOpResponse, FakeSpecialResponse)
}


class TransactionTests(unittest.TestCase):

    def test_request_serialize(self):
        txn = transaction.TransactionRequest()

        txn.add(FakeOpRequest(foo="bar"))
        txn.add(FakeSpecialRequest())

        result = txn.serialize(xid=20)

        expected = struct.pack(
            "!" + "".join([
                'i',  # xid
                'i',  # txn opcode
                'i?i',  # multiheader: opcode, done, error
                'i3s',  # three-character string
                'i?i',  # multiheader
                '',  # special request's body is blank
                'i?i',  # ending multiheader
            ]),
            *[
                20,
                14,
                99, False, -1,
                3, b"bar",
                999, False, -1,
                -1, True, -1,
            ]
        )

        self.assertEqual(result, expected)

    def test_request_stringified(self):
        txn = transaction.TransactionRequest()

        txn.add(FakeOpRequest(foo="bar"))
        txn.add(FakeSpecialRequest())

        self.assertEqual(
            str(txn),
            "Txn[FakeOpRequest(foo=bar), FakeSpecialRequest()]"
        )

    @patch.object(transaction, "response_xref", fake_response_xref)
    def test_response_deserialization(self):

        payload = struct.pack(
            "!" + "".join([
                'i?i',  # multiheader: opcode, done, error
                'i3s',  # three-character string
                'i?i',  # multiheader
                '',     # special request's body is blank
                'i?i',  # ending multiheader
            ]),
            *[
                99, False, -1,  # 99 = opcode for fake op
                3, b"bar",
                999, False, -1,  # 999 = opcode for fake special response
                -1, True, -1,
            ]
        )

        response = transaction.TransactionResponse.deserialize(payload)

        self.assertIsInstance(response.responses[0], FakeOpResponse)
        self.assertEqual(response.responses[0].foo, "bar")
        self.assertIsInstance(response.responses[1], FakeSpecialResponse)

    @patch.object(transaction, "response_xref", fake_response_xref)
    def test_response_error_deserialization(self):

        # since transactions are atomic, if one response is an error, they
        # will all be
        payload = struct.pack(
            "!" + "".join([
                'i?i',  # multiheader: opcode, done, error
                'i',    # error code
                'i?i',  # multiheader
                'i',    # error code
                'i?i',  # ending multiheader
            ]),
            *[
                -1, False, -1,
                0,  # 'rolled back' error
                -1, False, -1,
                -3,  # 'data inconsistency' error
                -1, True, -1,
            ]
        )

        response = transaction.TransactionResponse.deserialize(payload)

        self.assertIsInstance(response.responses[0], exc.RolledBack)
        self.assertIsInstance(response.responses[1], exc.DataInconsistency)

    @patch.object(transaction, "response_xref", fake_response_xref)
    def test_response_stringified(self):
        payload = struct.pack(
            "!" + "".join([
                'i?i',  # multiheader: opcode, done, error
                'i3s',  # three-character string
                'i?i',  # multiheader
                '',  # special request's body is blank
                'i?i',  # ending multiheader
            ]),
            *[
                99, False, -1,
                3, b"bar",
                999, False, -1,
                -1, True, -1,
            ]
        )

        response = transaction.TransactionResponse.deserialize(payload)

        self.assertEqual(
            str(response),
            "Txn[FakeOpResponse(foo=bar), FakeSpecialResponse()]"
        )
