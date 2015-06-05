import unittest

from mock import MagicMock as Mock

from oiopy import service
from oiopy import fakes


class ServiceTest(unittest.TestCase):
    def setUp(self):
        self.fake_api = fakes.FakeAPI()
        self.service = service.Service(self.fake_api, uri_base="/fake")

    def tearDown(self):
        self.service = None
        self.fake_api = None

    def test_action(self):
        srv = self.service
        srv.api.do_post = Mock()
        uri = "/fake"
        args = {"a": 1, "b": 2}
        srv._action(uri, "Fake", args)
        body = {"action": "Fake", "args": {"a": 1, "b": 2}}
        srv.api.do_post.assert_called_once_with("/fake/action", body=body,
                                                headers=None)
