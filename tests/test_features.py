import unittest

from zoonado import features


class FeaturesTests(unittest.TestCase):

    def test_three_four_version(self):
        available = features.Features((3, 4, 8))

        assert available.create_with_stat is False
        assert available.containers is False
        assert available.reconfigure is False

    def test_three_five_version(self):
        available = features.Features((3, 5, 0))

        assert available.create_with_stat is True
        assert available.containers is False
        assert available.reconfigure is True

    def test_three_five_point_version(self):
        available = features.Features((3, 5, 1))

        assert available.create_with_stat is True
        assert available.containers is True
        assert available.reconfigure is True
