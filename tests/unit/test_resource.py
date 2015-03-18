import unittest

from mock import MagicMock as Mock

from oiopy import resource
from oiopy import fakes
from oiopy import utils


class ResourceTest(unittest.TestCase):
    def setUp(self):
        data = {"name": "test_name", "id": utils.random_string(), "size": 1337}
        self.resource = resource.Resource(fakes.FakeService(), data)

    def _create_fake_resource(self):
        data = {"name": "test_name", "id": utils.random_string()}
        return resource.Resource(fakes.FakeService(), data)

    def tearDown(self):
        self.resource = None

    def test_add_data(self):
        res = self.resource
        data = {"a": 1, "b": 2}
        self.assertFalse(hasattr(res, "a"))
        self.assertFalse(hasattr(res, "b"))
        res._add_data(data)
        self.assertTrue(hasattr(res, "a"))
        self.assertTrue(hasattr(res, "b"))
        self.assertEqual(res.a, 1)
        self.assertEqual(res.b, 2)

    def test_getattr(self):
        res = self.resource
        self.assertRaises(AttributeError, res.__getattr__, "a")

    def test_delete(self):
        res = self.resource
        res.service.delete = Mock()
        res.delete()
        res.service.delete.assert_called_once_with(res)

    def test_reload(self):
        res = self.resource
        fake = self._create_fake_resource()
        fake._data["version"] = "0"
        res.service.get = Mock(return_value=fake)
        res.reload()
        self.assertEqual(res.version, "0")
        fake._data["version"] = "1.0"
        res.reload()
        self.assertEqual(res.version, "1.0")
