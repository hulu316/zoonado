import unittest

from zoonado.protocol import create


class CreateProtocolTests(unittest.TestCase):

    def test_set_flags_defaults_to_false(self):
        c = create.CreateRequest(path="/foo/bar", data=None, acl=[])

        c.set_flags()

        self.assertEqual(c.flags, 0)

    def test_setting_all_flags(self):
        c = create.CreateRequest(path="/foo/bar", data=None, acl=[])

        c.set_flags(ephemeral=True, sequential=True, container=True)

        self.assertEqual(c.flags, 7)

    def test_ephemeral_flag_is_first_bit(self):
        c = create.CreateRequest(path="/foo/bar", data=None, acl=[])

        c.set_flags(ephemeral=True)

        self.assertEqual(c.flags, 1)

    def test_sequential_flag_is_second_bit(self):
        c = create.CreateRequest(path="/foo/bar", data=None, acl=[])

        c.set_flags(sequential=True)

        self.assertEqual(c.flags, 2)

    def test_container_flag_is_third_bit(self):
        c = create.CreateRequest(path="/foo/bar", data=None, acl=[])

        c.set_flags(container=True)

        self.assertEqual(c.flags, 4)
