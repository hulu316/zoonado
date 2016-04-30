# -*- coding: utf-8 -*-
import struct
import unittest

from zoonado.protocol import primitives


class PrimitivesTests(unittest.TestCase):

    def test_basic_equality(self):
        int1 = primitives.Int(8)
        int2 = primitives.Int(8)

        assert int1 == int2

    def test_basic_string_repr(self):
        self.assertEqual(str(primitives.Float(1.3)), "Float(1.3)")

    def test_basic_parse_return_tuple(self):
        a_long = primitives.Long(3.33)

        fmt, values = a_long.render()

        self.assertEqual(fmt, "q")
        self.assertEqual(values, [3.33])

    def test_variable_render_value_must_be_defined(self):
        v = primitives.VariablePrimitive([])

        with self.assertRaises(NotImplementedError):
            v.render_value(["foo"])

    def test_variable_parse_value_must_be_defined(self):
        with self.assertRaises(NotImplementedError):
            primitives.VariablePrimitive.parse_value(["bar"])

    def test_vector_rendering(self):
        v = primitives.Vector.of(primitives.Double)([1, 2.9, 3, 4.0])

        fmt, values = v.render()

        self.assertEqual(fmt, "idddd")
        self.assertEqual(values, [4, 1, 2.9, 3, 4.0])

    def test_empty_vector_rendering(self):
        v = primitives.Vector.of(primitives.Double)([])

        fmt, values = v.render()

        self.assertEqual(fmt, "i")
        self.assertEqual(values, [0])

    def test_none_vector_rendering(self):
        v = primitives.Vector.of(primitives.Double)(None)

        fmt, values = v.render()

        self.assertEqual(fmt, "i")
        self.assertEqual(values, [0])

    def test_vector_parsing(self):
        raw = struct.pack("!iddd", 3, 100, 1440, 1200 * 1200)

        print(struct.unpack("!iddd", raw))
        values, new_offset = primitives.Vector.of(primitives.Double).parse(
            raw, offset=0
        )

        self.assertEqual(values, [100.0, 1440.0, 1200.0 * 1200])
        self.assertEqual(new_offset, struct.calcsize("!iddd"))

    def test_string_rendering(self):
        s = primitives.UString(u"foobar")

        fmt, values = s.render()

        self.assertEqual(fmt, "i6s")
        self.assertEqual(values, [6, b"foobar"])

    def test_none_string_rendering(self):
        s = primitives.UString(None)

        fmt, values = s.render()

        self.assertEqual(fmt, "i")
        self.assertEqual(values, [-1])

    def test_none_string_parsing(self):
        raw = struct.pack("!i", -1)

        value, new_offset = primitives.UString.parse(raw, offset=0)

        self.assertEqual(value, None)
        self.assertEqual(new_offset, 4)

    def test_blank_string_rendering(self):
        s = primitives.UString("")

        fmt, values = s.render()

        self.assertEqual(fmt, "i0s")
        self.assertEqual(values, [0, b""])

    def test_buffer_rendering(self):
        b = primitives.Buffer(b'asdf')

        fmt, values = b.render()

        self.assertEqual(fmt, "i4s")
        self.assertEqual(values, [4, b"asdf"])

    def test_empty_buffer_rendering(self):
        b = primitives.Buffer(None)

        fmt, values = b.render()

        self.assertEqual(fmt, "i")
        self.assertEqual(values, [-1])

    def test_buffer_parsing(self):
        raw = struct.pack("!i3s", 3, b"fo\xc3")

        value, new_offset = primitives.Buffer.parse(raw, offset=0)

        self.assertEqual(value, b"fo\xc3")
        self.assertEqual(new_offset, 7)

    def test_ustring_string(self):
        s = primitives.UString(u"foobar")

        self.assertEqual(str(s), str(u"foobar"))

    def test_vector_string(self):
        a = primitives.Vector.of(primitives.Int)([1, 3, 6, 9])

        self.assertEqual(str(a), "Int[1, 3, 6, 9]")

    def test_ustring_render_parse_is_stable(self):
        s = primitives.UString(u"foobar")

        fmt, values = s.render()

        raw = struct.pack("!" + fmt, *values)

        value, _ = primitives.UString.parse(raw, 0)

        self.assertEqual(value, u"foobar")

    def test_ustring_render_parse_handles_nonstrings(self):
        s = primitives.UString(123)

        fmt, values = s.render()

        raw = struct.pack("!" + fmt, *values)

        value, _ = primitives.UString.parse(raw, 0)

        self.assertEqual(value, u"123")
