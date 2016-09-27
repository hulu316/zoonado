# -*- coding: utf-8 -*-
import struct
import unittest

from zoonado.protocol import part, primitives


class PartTests(unittest.TestCase):

    def test_instantiation(self):

        class FakePart(part.Part):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
                ("third", primitives.Float),
            )

        p = FakePart(first=8, second=u"foobar")

        self.assertEqual(p.first, 8)
        self.assertEqual(p.second, u"foobar")

    def test_unset_fields_set_to_none(self):

        class FakePart(part.Part):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
            )

        p = FakePart(second=u"bazz")

        self.assertEqual(p.first, None)

    def test_passing_unknown_named_field(self):

        class FakePart(part.Part):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
            )

        with self.assertRaises(ValueError):
            FakePart(other=5.5)

    def test_simple_rendering(self):

        class FakePart(part.Part):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
                ("third", primitives.Float),
            )

        p = FakePart(first=8, second=u"foobar")

        fmt, data = p.render()

        self.assertEqual(fmt, "ii6sf")
        self.assertEqual(data, [8, 6, b"foobar", None])

    def test_render_some_field(self):

        class FakePart(part.Part):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
                ("third", primitives.Float),
            )

        p = FakePart(first=8, second=u"foobar", third=3.3)

        fmt, data = p.render([FakePart.parts[0], FakePart.parts[2]])

        self.assertEqual(fmt, "if")
        self.assertEqual(data, [8, 3.3])

    def test_render_with_nested_parts(self):

        class NestedPart(part.Part):
            parts = (
                ("left", primitives.UString),
                ("right", primitives.UString),
            )

        class FakePart(part.Part):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
                ("third", NestedPart),
            )

        p = FakePart(
            second=u"foobar", third=NestedPart(left="up", right="down")
        )

        fmt, data = p.render()

        self.assertEqual(fmt, "ii6si2si4s")
        self.assertEqual(data, [None, 6, b"foobar", 2, b"up", 4, b"down"])

    def test_parse(self):

        class NestedPart(part.Part):
            parts = (
                ("left", primitives.UString),
                ("right", primitives.UString),
            )

        class FakePart(part.Part):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
                ("third", NestedPart),
            )

        raw = struct.pack(
            "!ii6si2si4s", 8, 6, b"foobar", 2, b"up", 4, b"down"
        )

        p, new_offset = FakePart.parse(raw, offset=0)

        self.assertEqual(new_offset, struct.calcsize("!ii6si2si4s"))

        self.assertEqual(p.first, 8)
        self.assertEqual(p.second, u"foobar")
        self.assertEqual(p.third, NestedPart(left=u"up", right=u"down"))

    def test_parse_with_unicode(self):

        class NestedPart(part.Part):
            parts = (
                ("left", primitives.UString),
                ("right", primitives.UString),
            )

        class FakePart(part.Part):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
                ("third", NestedPart),
            )

        raw = struct.pack(
            "!ii7si2si4s", 8, 7, b"fo\xc3\xb8bar", 2, b"up", 4, b"down"
        )

        p, new_offset = FakePart.parse(raw, offset=0)

        self.assertEqual(new_offset, struct.calcsize("!ii7si2si4s"))

        self.assertEqual(p.first, 8)
        self.assertEqual(p.second, u"foøbar")
        self.assertEqual(p.third, NestedPart(left=u"up", right=u"down"))

    def test_equality(self):

        class NestedPart(part.Part):
            parts = (
                ("left", primitives.UString),
                ("right", primitives.UString),
            )

        class FakePart(part.Part):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
                ("third", NestedPart),
            )

        p1 = FakePart(
            second=u"foobar",
            third=NestedPart(left="up", right="down"),
        )

        p2 = FakePart(
            third=NestedPart(right="down", left="up"),
            second=u"foobar",
        )

        self.assertEqual(p1, p2)

    def test_inequality(self):

        class NestedPart(part.Part):
            parts = (
                ("left", primitives.UString),
                ("right", primitives.UString),
            )

        class FakePart(part.Part):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.UString),
                ("third", NestedPart),
            )

        p1 = FakePart(
            second=u"foobar",
            third=NestedPart(left="up", right="down"),
        )

        p2 = FakePart(
            third=NestedPart(left="down", right="up"),
            second=u"foobar",
        )

        self.assertNotEqual(p1, p2)

    def test_string_repr(self):

        class NestedPart(part.Part):
            parts = (
                ("left", primitives.UString),
                ("right", primitives.UString),
            )

        class FakePart(part.Part):
            parts = (
                ("first", primitives.Int),
                ("second", primitives.Vector.of(primitives.UString)),
                ("third", NestedPart),
            )

        p = FakePart(
            second=[u"foobar", u"bleebloo"],
            third=NestedPart(left="up", right="down"),
        )

        self.assertEqual(
            str(p),
            "FakePart(" +
            "first=None, " +
            "second=[foobar, bleebloo], " +
            "third=NestedPart(left=up, right=down))"
        )
