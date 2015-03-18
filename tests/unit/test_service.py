import unittest

from mock import MagicMock as Mock

from oiopy import service
from oiopy import fakes
from oiopy import utils


class ServiceTest(unittest.TestCase):
    def setUp(self):
        self.fake_api = fakes.FakeAPI()
        self.service = service.Service(self.fake_api, uri_base="/fake")

    def tearDown(self):
        self.service = None
        self.fake_api = None

    def test_make_uri(self):
        srv = self.service
        random_id = utils.random_string()

        class DummyObj(object):
            id = random_id

        obj = DummyObj()
        uri = srv._make_uri(obj)
        self.assertEqual(uri, "/fake/%s" % random_id)
        uri = srv._make_uri(random_id)
        self.assertEqual(uri, "/fake/%s" % random_id)

        id = "foo/a ?+"
        uri = srv._make_uri(id)
        self.assertEqual(uri, "/fake/%s" % utils.quote(id, ''))

    def test_action(self):
        srv = self.service
        srv.api.do_post = Mock()
        uri = "/fake"
        args = {"a": 1, "b": 2}
        srv._action(uri, "Fake", args)
        body = {"action": "Fake", "args": {"a": 1, "b": 2}}
        srv.api.do_post.assert_called_once_with("/fake/action", body=body,
                                                headers=None)
