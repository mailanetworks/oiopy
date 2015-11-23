from eventlet import Timeout
from contextlib import contextmanager
from cStringIO import StringIO
import json
from mock import MagicMock as Mock
from mock import patch
import random
import unittest


import oiopy
from oiopy import exceptions
from oiopy import fakes
from oiopy import utils
from oiopy.object_storage import container_headers
from oiopy.object_storage import handle_object_not_found
from oiopy.object_storage import handle_container_not_found
from oiopy.object_storage import object_headers
from oiopy.object_storage import _sort_chunks
from oiopy.object_storage import ChunkDownloadHandler
from oiopy.object_storage import ChunkReadTimeout


@contextmanager
def set_http_connect(*args, **kwargs):
    old = oiopy.object_storage.http_connect

    new = fakes.fake_http_connect(*args, **kwargs)
    try:
        oiopy.object_storage.http_connect = new
        yield new
        unused_status = list(new.status_iter)
        if unused_status:
            raise AssertionError('unused status %r' % unused_status)

    finally:
        oiopy.object_storage.http_connect = old


def empty_stream():
    return StringIO("")


class ObjectStorageTest(unittest.TestCase):
    def setUp(self):
        self.api = fakes.FakeStorageAPI("NS", "http://1.2.3.4:8000")
        self.account = "test"
        self.container = "fake"
        self.headers = {"x-req-id": utils.random_string()}
        self.uri_base = "NS"

    def test_handle_container_not_found(self):
        @handle_container_not_found
        def test(self, account, container):
            raise exceptions.NotFound("No container")

        container = utils.random_string()
        self.assertRaises(
            exceptions.NoSuchContainer, test, self, self.account, container)

    def test_handle_object_not_found(self):
        @handle_object_not_found
        def test(self, account, container, obj):
            raise exceptions.NotFound("No object")

        obj = utils.random_string()
        self.assertRaises(
            exceptions.NoSuchObject, test, self, self.account, self.container,
            obj)

    def test_container_list(self):
        resp = fakes.FakeResponse()
        name = utils.random_string()
        marker = utils.random_string()
        delimiter = utils.random_string()
        end_marker = utils.random_string()
        prefix = utils.random_string()
        limit = random.randint(1, 1000)
        body = {"listing": [[name, 0, 0, 0]]}
        self.api._request = Mock(return_value=(resp, body))
        self.api._get_account_url = Mock(return_value='fake_endpoint')
        containers, meta = self.api.container_list(
            self.account, limit=limit, marker=marker, prefix=prefix,
            delimiter=delimiter, end_marker=end_marker, headers=self.headers)
        params = {"id": self.account, "prefix": prefix, "delimiter": delimiter,
                  "marker": marker, "end_marker": end_marker, "limit": limit}
        uri = "v1.0/account/containers"
        self.api._request.assert_called_once_with(
            'GET', uri, endpoint='fake_endpoint', params=params,
            headers=self.headers)
        self.assertEqual(len(containers), 1)

    def test_object_list(self):
        api = self.api
        marker = utils.random_string()
        delimiter = utils.random_string()
        end_marker = utils.random_string()
        prefix = utils.random_string()
        limit = random.randint(1, 1000)
        name0 = utils.random_string()
        name1 = utils.random_string()
        resp_body = {"objects": [{"name": name0}, {"name": name1}]}
        api._request = Mock(return_value=(None, resp_body))
        l = api.object_list(
            self.account, self.container, limit=limit, marker=marker,
            prefix=prefix, delimiter=delimiter, end_marker=end_marker,
            headers=self.headers)
        uri = "%s/container/list" % self.uri_base
        params = {'acct': self.account, 'ref': self.container,
                  'marker': marker, 'max': limit,
                  'delimiter': delimiter, 'prefix': prefix,
                  'end_marker': end_marker}
        api._request.assert_called_once_with(
            'GET', uri, params=params, headers=self.headers)
        self.assertEqual(len(l['objects']), 2)

    def test_container_show(self):
        api = self.api
        resp = fakes.FakeResponse()
        name = utils.random_string()
        cont_size = random.randint(1, 1000)
        resp.headers = {
            container_headers["size"]: cont_size
        }
        api._request = Mock(return_value=(resp, {}))
        info = api.container_show(self.account, name, headers=self.headers)
        uri = "%s/container/get_properties" % self.uri_base
        params = {'acct': self.account, 'ref': name}
        api._request.assert_called_once_with(
            'POST', uri, params=params, headers=self.headers)
        self.assertEqual(info, {})

    def test_container_show_not_found(self):
        api = self.api
        api._request = Mock(side_effect=exceptions.NotFound("No container"))
        name = utils.random_string()
        self.assertRaises(exceptions.NoSuchContainer, api.container_show,
                          self.account, name)

    def test_container_create(self):
        api = self.api
        resp = fakes.FakeResponse()
        resp.status_code = 204
        api._request = Mock(return_value=(resp, None))

        name = utils.random_string()
        result = api.container_create(self.account, name, headers=self.headers)
        self.assertEqual(result, True)

        uri = "%s/container/create" % self.uri_base
        params = {'acct': self.account, 'ref': name}
        self.headers['x-oio-action-mode'] = 'autocreate'
        api._request.assert_called_once_with(
            'POST', uri, params=params, headers=self.headers)

    def test_container_create_exist(self):
        api = self.api
        resp = fakes.FakeResponse()
        resp.status_code = 201
        api._request = Mock(return_value=(resp, None))

        name = utils.random_string()
        result = api.container_create(self.account, name)
        self.assertEqual(result, False)

    def test_container_delete(self):
        api = self.api

        resp = fakes.FakeResponse()
        resp.status_code = 204
        api._request = Mock(return_value=(resp, None))
        api.directory.unlink = Mock(return_value=None)
        name = utils.random_string()
        api.container_delete(self.account, name, headers=self.headers)

        api.directory.unlink.assert_called_once_with(
            self.account, name, "meta2", headers=self.headers)
        uri = "%s/container/destroy" % self.uri_base
        params = {'acct': self.account, 'ref': name}
        api._request.assert_called_once_with(
            'POST', uri, params=params, headers=self.headers)

    def test_container_delete_not_empty(self):
        api = self.api

        api._request = Mock(side_effect=exceptions.Conflict(""))
        api.directory.unlink = Mock(return_value=None)
        name = utils.random_string()

        self.assertRaises(
            exceptions.ContainerNotEmpty, api.container_delete, self.account,
            name)

    def test_container_update(self):
        api = self.api

        name = utils.random_string()
        key = utils.random_string()
        value = utils.random_string()
        meta = {key: value}
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        api.container_update(self.account, name, meta, headers=self.headers)

        data = json.dumps(meta)
        uri = "%s/container/set_properties" % self.uri_base
        params = {'acct': self.account, 'ref': name}
        api._request.assert_called_once_with(
            'POST', uri, data=data, params=params, headers=self.headers)

    def test_object_show(self):
        api = self.api
        name = utils.random_string()
        size = random.randint(1, 1000)
        content_hash = utils.random_string()
        content_type = utils.random_string()
        resp = fakes.FakeResponse()
        resp.headers = {object_headers["name"]: name,
                        object_headers["size"]: size,
                        object_headers["hash"]: content_hash,
                        object_headers["mime_type"]: content_type}
        api._request = Mock(return_value=(resp, {}))
        obj = api.object_show(
            self.account, self.container, name, headers=self.headers)

        uri = "%s/content/get_properties" % self.uri_base
        params = {'acct': self.account, 'ref': self.container,
                  'path': name}
        api._request.assert_called_once_with(
            'POST', uri, params=params, headers=self.headers)
        self.assertIsNotNone(obj)

    def test_object_create_no_data(self):
        api = self.api
        name = utils.random_string()
        self.assertRaises(exceptions.MissingData, api.object_create,
                          self.account, self.container, obj_name=name)

    def test_object_create_no_name(self):
        api = self.api
        self.assertRaises(exceptions.MissingName, api.object_create,
                          self.account, self.container, data="x")

    def test_object_create_no_content_length(self):
        api = self.api
        name = utils.random_string()
        f = Mock()
        self.assertRaises(
            exceptions.MissingContentLength, api.object_create, self.account,
            self.container, f, obj_name=name)

    def test_object_create_missing_file(self):
        api = self.api
        name = utils.random_string()
        self.assertRaises(
            exceptions.FileNotFound, api.object_create, self.account,
            self.container, name)

    def test_object_update(self):
        api = self.api

        name = utils.random_string()
        key = utils.random_string()
        value = utils.random_string()
        meta = {key: value}
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        api.object_update(
            self.account, self.container, name, meta, headers=self.headers)

        data = json.dumps(meta)
        uri = "%s/content/set_properties" % self.uri_base
        params = {'acct': self.account, 'ref': self.container,
                  'path': name}
        api._request.assert_called_once_with(
            'POST', uri, data=data, params=params, headers=self.headers)

    def test_object_delete(self):
        api = self.api
        name = utils.random_string()
        resp_body = [
            {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32},
            {"url": "http://1.2.3.4:6000/BBBB", "pos": "1", "size": 32},
            {"url": "http://1.2.3.4:6000/CCCC", "pos": "2", "size": 32}
        ]
        api._request = Mock(return_value=(None, resp_body))

        api.object_delete(
            self.account, self.container, name, headers=self.headers)

        uri = "%s/content/delete" % self.uri_base
        params = {'acct': self.account, 'ref': self.container,
                  'path': name}
        api._request.assert_called_once_with(
            'POST', uri, params=params, headers=self.headers)

    def test_object_delete_not_found(self):
        api = self.api
        name = utils.random_string()
        api._request = Mock(side_effect=exceptions.NotFound("No object"))
        self.assertRaises(
            exceptions.NoSuchObject, api.object_delete, self.account,
            self.container, name)

    def test_object_store(self):
        api = self.api
        name = utils.random_string()
        raw_chunks = [
            {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32},
            {"url": "http://1.2.3.4:6000/BBBB", "pos": "1", "size": 32},
            {"url": "http://1.2.3.4:6000/CCCC", "pos": "2", "size": 32}
        ]
        meta = {object_headers['id']: utils.random_string(),
                object_headers['version']: utils.random_string()}
        api._content_prepare = Mock(return_value=(meta, raw_chunks))
        api._content_create = Mock(return_value=({}, {}))
        with set_http_connect(201, 201, 201):
            api.object_create(
                self.account, self.container, obj_name=name, data="x",
                headers=self.headers)

    def test_sort_chunks(self):
        raw_chunks = [
            {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32},
            {"url": "http://1.2.3.4:6000/BBBB", "pos": "0", "size": 32},
            {"url": "http://1.2.3.4:6000/CCCC", "pos": "1", "size": 32},
            {"url": "http://1.2.3.4:6000/DDDD", "pos": "1", "size": 32},
            {"url": "http://1.2.3.4:6000/EEEE", "pos": "2", "size": 32},
            {"url": "http://1.2.3.4:6000/FFFF", "pos": "2", "size": 32},
        ]
        chunks = _sort_chunks(raw_chunks, False)
        sorted_chunks = {
            0: [
                {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32},
                {"url": "http://1.2.3.4:6000/BBBB", "pos": "0", "size": 32}],
            1: [
                {"url": "http://1.2.3.4:6000/CCCC", "pos": "1", "size": 32},
                {"url": "http://1.2.3.4:6000/DDDD", "pos": "1", "size": 32}],
            2: [
                {"url": "http://1.2.3.4:6000/EEEE", "pos": "2", "size": 32},
                {"url": "http://1.2.3.4:6000/FFFF", "pos": "2", "size": 32}
            ]}
        self.assertEqual(chunks, sorted_chunks)
        raw_chunks = [
            {"url": "http://1.2.3.4:6000/AAAA", "pos": "0.0", "size": 32},
            {"url": "http://1.2.3.4:6000/BBBB", "pos": "0.1", "size": 32},
            {"url": "http://1.2.3.4:6000/CCCC", "pos": "0.p0", "size": 32},
            {"url": "http://1.2.3.4:6000/DDDD", "pos": "1.0", "size": 32},
            {"url": "http://1.2.3.4:6000/EEEE", "pos": "1.1", "size": 32},
            {"url": "http://1.2.3.4:6000/FFFF", "pos": "1.p0", "size": 32},
        ]
        chunks = _sort_chunks(raw_chunks, True)
        sorted_chunks = {
            0: {
                "0": {
                    "url": "http://1.2.3.4:6000/AAAA", "pos": "0.0",
                    "size": 32},
                "1": {"url": "http://1.2.3.4:6000/BBBB", "pos": "0.1",
                      "size": 32},
                "p0": {"url": "http://1.2.3.4:6000/CCCC", "pos": "0.p0",
                       "size": 32}
            },
            1: {
                "0": {"url": "http://1.2.3.4:6000/DDDD", "pos": "1.0",
                      "size": 32},
                "1": {"url": "http://1.2.3.4:6000/EEEE", "pos": "1.1",
                      "size": 32},
                "p0": {"url": "http://1.2.3.4:6000/FFFF", "pos": "1.p0",
                       "size": 32}
            }}
        self.assertEqual(chunks, sorted_chunks)

    def test_put_stream_empty(self):
        api = self.api
        name = utils.random_string()
        chunks = {
            0: [
                {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32},
                {"url": "http://1.2.3.4:6000/BBBB", "pos": "0", "size": 32},
                {"url": "http://1.2.3.4:6000/CCCC", "pos": "0", "size": 32}
            ]
        }
        src = empty_stream()
        sysmeta = {'content_length': 0,
                   'id': utils.random_string(),
                   'version': utils.random_string()}

        with set_http_connect(201, 201, 201):
            chunks, bytes_transferred, content_checksum = api._put_stream(
                self.account, self.container, name, src, sysmeta, chunks)

        final_chunks = [
            {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 0,
             "hash": "d41d8cd98f00b204e9800998ecf8427e"},
            {"url": "http://1.2.3.4:6000/BBBB", "pos": "0", "size": 0,
             "hash": "d41d8cd98f00b204e9800998ecf8427e"},
            {"url": "http://1.2.3.4:6000/CCCC", "pos": "0", "size": 0,
             "hash": "d41d8cd98f00b204e9800998ecf8427e"}
        ]
        self.assertEqual(final_chunks, chunks)
        self.assertEqual(bytes_transferred, 0)
        self.assertEqual(content_checksum, "d41d8cd98f00b204e9800998ecf8427e")

    def test_put_stream_connect_exception(self):
        api = self.api
        name = utils.random_string()
        chunks = {
            0: [
                {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32},
                {"url": "http://1.2.3.4:6000/BBBB", "pos": "0", "size": 32},
                {"url": "http://1.2.3.4:6000/CCCC", "pos": "0", "size": 32}
            ]
        }
        src = empty_stream()
        sysmeta = {'content_length': 0,
                   'id': utils.random_string(),
                   'version': utils.random_string()}

        with set_http_connect(201, Exception(), Exception()):
            chunks, bytes_transferred, content_checksum = api._put_stream(
                self.account, self.container, name, src, sysmeta, chunks)
        self.assertEqual(len(chunks), 1)
        chunk = {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 0,
                 "hash": "d41d8cd98f00b204e9800998ecf8427e"}
        self.assertEqual(chunk, chunks[0])

    def test_put_stream_connect_timeout(self):
        api = self.api
        name = utils.random_string()
        chunks = {
            0: [
                {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32}
            ]
        }
        src = empty_stream()
        sysmeta = {'content_length': 0,
                   'id': utils.random_string(),
                   'version': utils.random_string()}

        with set_http_connect(200, slow_connect=True):
            chunks, bytes_transferred, content_checksum = api._put_stream(
                self.account, self.container, name, src, sysmeta, chunks)

    def test_put_stream_client_timeout(self):
        api = self.api
        name = utils.random_string()
        chunks = {
            0: [
                {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32}
            ]
        }

        src = fakes.FakeTimeoutStream(5)
        sysmeta = {'content_length': 0,
                   'id': utils.random_string(),
                   'version': utils.random_string()}

        with set_http_connect(200):
            self.assertRaises(
                exceptions.ClientReadTimeout, api._put_stream, self.account,
                self.container, name, src, sysmeta, chunks)


class TestSource(object):
    def __init__(self, parts):
        self.parts = list(parts)

    def read(self, chunk_size):
        if self.parts:
            part = self.parts.pop(0)
            if part is None:
                raise ChunkReadTimeout()
            else:
                return part
        else:
            return ''


class TestChunkDownloadHandler(unittest.TestCase):
    def setUp(self):
        super(TestChunkDownloadHandler, self).setUp()
        self.chunks = [
            {'url': 'http://1.2.3.4:6000/AAAA', 'pos': '0', 'size': 32},
            {'url': 'http://1.2.3.4:6001/BBBB', 'pos': '0', 'size': 32}]
        self.size = 32
        self.offset = 0
        self.handler = ChunkDownloadHandler(
            self.chunks, self.size, self.offset)

    @patch('oiopy.object_storage.close_source')
    @patch('oiopy.object_storage.ChunkDownloadHandler._get_chunk_source')
    def test_get_stream(self, mock_chunk_source, mock_close):
        parts = ('foo', 'bar')
        source = TestSource(parts)
        mock_chunk_source.return_value = source

        stream = self.handler.get_stream()

        d = ''.join(stream)
        self.assertTrue(d, 'foobar')
        mock_close.assert_called_once_with(source)

    @patch('oiopy.object_storage.close_source')
    def test_make_stream(self, mock_close):
        source = TestSource(('foo', 'bar'))
        stream = self.handler._make_stream(source)
        d = ''.join(stream)
        self.assertTrue(d, 'foobar')
        mock_close.assert_called_once_with(source)

    @patch('oiopy.object_storage.close_source')
    def test_make_stream_timeout(self, mock_close):
        h = self.handler
        source = TestSource(('foo', None))
        source2 = TestSource(('bar',))
        h._fast_forward = Mock()
        stream = h._make_stream(source)
        with patch.object(h, '_get_chunk_source', lambda: source2):
            parts = list(stream)
        self.assertEqual(parts, ['foo', 'bar'])
        h._fast_forward.assert_called_once_with(len('foo'))
        mock_close.assert_any_call(source)
        mock_close.assert_any_call(source2)
        self.assertEqual(mock_close.call_count, 2)

    @patch('oiopy.object_storage.close_source')
    def test_make_stream_timeout_resume_failure(self, mock_close):
        h = self.handler
        source = TestSource(('foo', None))
        h._fast_forward = Mock()
        stream = h._make_stream(source)
        with patch.object(h, '_get_chunk_source', lambda: None):
            self.assertEqual('foo', next(stream))
            with self.assertRaises(ChunkReadTimeout):
                next(stream)
        h._fast_forward.assert_called_once_with(len('foo'))
        mock_close.assert_called_once_with(source)

    def test_get_chunk_source(self):
        with set_http_connect(200, body='foobar'):
            source = self.handler._get_chunk_source()
        self.assertEqual('foobar', source.read())

    def test_get_chunk_source_resume_timeout(self):
        h = self.handler
        with set_http_connect(Timeout(), 200, body='foobar'):
            source = h._get_chunk_source()
        self.assertEqual('foobar', source.read())
        self.assertEqual(h.failed_chunks, [self.chunks[0]])

    def test_get_chunk_source_resume_404(self):
        h = self.handler
        with set_http_connect(404, 200, body='foobar'):
            source = h._get_chunk_source()

        self.assertEqual('foobar', source.read())
        self.assertEqual(h.failed_chunks, [self.chunks[0]])
