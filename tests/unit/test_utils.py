import unittest

from oiopy import utils


class UtilsTest(unittest.TestCase):
    def test_get_id(self):
        random_id = utils.random_string()

        class DummyObj(object):
            id = random_id

        obj = DummyObj()
        self.assertEqual(utils.get_id(obj), random_id)
        self.assertEqual(utils.get_id(obj.id), random_id)
        dummy = object()
        self.assertEqual(utils.get_id(dummy), dummy)