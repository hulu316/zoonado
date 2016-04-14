import unittest

from zoonado.protocol.acl import ACL

from zoonado import client


class ClientTests(unittest.TestCase):

    def test_default_acl_is_unrestricted(self):
        c = client.Zoonado("host,host,host")

        self.assertEqual(len(c.default_acl), 1)

        self.assertEqual(
            c.default_acl[0],
            ACL.make(
                scheme="world", id="anyone",
                read=True, write=True, create=True, delete=True, admin=True
            )
        )
