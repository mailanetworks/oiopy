import unittest
from contextlib import contextmanager

from mock import MagicMock as Mock

from oiopy import api
from oiopy import fakes
from oiopy import exceptions
import oiopy.api


FAKE_URL = "http://localhost:8888"


@contextmanager
def set_proxyd_request(*args, **kwargs):
    old = oiopy.api.API._http_request

    new = fakes.fake_http_request(*args, **kwargs)
    try:
        oiopy.api.API._http_request = new
        yield new
        unused_status = list(new.status_iter)
        if unused_status:
            raise AssertionError('unused status %r' % unused_status)

    finally:
        oiopy.api.API._proxyd_request = old


class APITest(unittest.TestCase):
    def setUp(self):
        self.api = api.API(FAKE_URL)
        self.api._service = fakes.FakeService()

    def tearDown(self):
        self.api = None

    def test_create(self):
        srv = self.api._service
        srv.create = Mock()
        self.api.create("id")
        srv.create.assert_called_once_with("id")

    def test_get(self):
        srv = self.api._service
        srv.get = Mock()
        self.api.get("id")
        srv.get.assert_called_once_with("id")

    def test_delete(self):
        srv = self.api._service
        srv.delete = Mock()
        self.api.delete("id")
        srv.delete.assert_called_once_with("id")

    def test_list(self):
        srv = self.api._service
        srv.list = Mock()
        self.api.list()
        srv.list.assert_called_once_with(limit=None, marker=None)

    def test_do_head(self):
        self.api._request = Mock()
        self.api.do_head(FAKE_URL)
        self.api._request.assert_called_once_with(FAKE_URL, "HEAD")

    def test_do_delete(self):
        self.api._request = Mock()
        self.api.do_delete(FAKE_URL)
        self.api._request.assert_called_once_with(FAKE_URL, "DELETE")

    def test_do_put(self):
        self.api._request = Mock()
        self.api.do_put(FAKE_URL)
        self.api._request.assert_called_once_with(FAKE_URL, "PUT")

    def test_do_get(self):
        self.api._request = Mock()
        self.api.do_get(FAKE_URL)
        self.api._request.assert_called_once_with(FAKE_URL, "GET")

    def test_do_copy(self):
        self.api._request = Mock()
        self.api.do_copy(FAKE_URL)
        self.api._request.assert_called_once_with(FAKE_URL, "COPY")

    def test_do_post(self):
        self.api._request = Mock()
        self.api.do_post(FAKE_URL)
        self.api._request.assert_called_once_with(FAKE_URL, "POST")

    def test_request_ok(self):
        uri = "/fake"
        with set_proxyd_request(200):
            resp, resp_body = self.api._request(uri, "GET")
        self.assertEqual(resp.status_code, 200)

    def test_request_with_body(self):
        uri = "/fake"
        body = {"a": 1, "b": 2}
        context = {}

        def cb(host, method, path, body=None, headers=None):
            context['method'] = method
            context['path'] = path
            context['headers'] = headers
            context['body'] = body

        with set_proxyd_request(200, callback=cb):
            resp, resp_body = self.api._request(uri, "PUT", body=body)

        self.assertEqual(context['method'], "PUT")
        self.assertEqual(context['body'], '{"a": 1, "b": 2}')
        self.assertEqual(context['headers'], None)

    def test_request_response(self):
        uri = "/fake"
        with set_proxyd_request((200, '{"a": 1, "b": 2}')):
            resp, resp_body = self.api._request(uri, "GET")
        self.assertEqual(resp_body, {"a": 1, "b": 2})

        uri = '/fake2'
        with set_proxyd_request((200, 'data')):
            resp, resp_body = self.api._request(uri, "GET")
        self.assertEqual(resp_body, 'data')

    def test_request_400(self):
        uri = "/fake"
        with set_proxyd_request(400):
            self.assertRaises(exceptions.ClientException, self.api._request,
                              uri, "GET")



