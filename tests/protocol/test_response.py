import struct
import unittest

from zoonado.protocol import response, primitives


class ResponseTests(unittest.TestCase):

    def test_deserialize(self):

        class FakeResponse(response.Response):
            opcode = 99

            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
            )

        # note that the xid and opcode are omitted, they're part of a preamble
        # that a connection would use to determine which Response to use
        # for deserializing
        raw = struct.pack("!ii6s", 3, 6, b"foobar")

        result = FakeResponse.deserialize(raw)

        self.assertEqual(result.first, 3)
        self.assertEqual(result.second, u"foobar")
