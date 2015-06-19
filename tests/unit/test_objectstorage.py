import unittest
import random
from cStringIO import StringIO
from contextlib import contextmanager

from mock import MagicMock as Mock

import oiopy
from oiopy import utils
from oiopy import fakes
from oiopy import exceptions
from oiopy.object_storage import container_headers, object_headers
from oiopy.object_storage import Container
from oiopy.object_storage import StorageObject
from oiopy.object_storage import ensure_container
from oiopy.object_storage import handle_object_not_found
from oiopy.object_storage import handle_container_not_found
from oiopy.object_storage import OBJECT_METADATA_PREFIX


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
        self.api = fakes.FakeStorageAPI("http://1.2.3.4:8000", "NS")
        self.account = "AUTH_test"
        self.container = self.api.create(self.account, "fake")
        self.headers = {"x-req-id": utils.random_string()}
        self.uri_base = "/m2/NS/%s" % self.account

    def test_ensure_container(self):
        class TestAPI(object):
            _service = fakes.FakeService()

            @ensure_container
            def test_method(self, account, container):
                return container

        api = TestAPI()
        api._make = Mock(return_value=self.container)
        ct = api.test_method(self.account, self.container)
        self.assertTrue(ct is self.container)
        ct = api.test_method(self.account, self.container.name)
        self.assertTrue(ct is self.container)

    def test_handle_container_not_found(self):
        @handle_container_not_found
        def test(self, account, container):
            raise exceptions.NotFound("No container")

        container = utils.random_string()
        self.assertRaises(exceptions.NoSuchContainer, test, self, self.account,
                          container)

    def test_handle_object_not_found(self):
        @handle_object_not_found
        def test(self, obj):
            raise exceptions.NotFound("No object")

        obj = utils.random_string()
        self.assertRaises(exceptions.NoSuchObject, test, self, obj)

    def test_api_list_containers(self):
        resp = fakes.FakeResponse()
        name = utils.random_string()
        marker = utils.random_string()
        delimiter = utils.random_string()
        end_marker = utils.random_string()
        prefix = utils.random_string()
        limit = random.randint(1, 1000)
        body = {"listing": [[name, 0, 0, 0]]}
        self.api._request = Mock(return_value=(resp, body))
        self.api.get_account_url = Mock(return_value='')
        containers, meta = self.api.list_containers(self.account, limit=limit,
                                                    marker=marker,
                                                    prefix=prefix,
                                                    delimiter=delimiter,
                                                    end_marker=end_marker,
                                                    headers=self.headers)
        qs = "end_marker=%s&prefix=%s&delimiter=%s&limit=%s&marker=%s&id=%s" % (
            end_marker, prefix, delimiter, limit, marker, self.account)
        uri = "/v1.0/account/containers?%s" % qs
        self.api._request.assert_called_once_with(uri, 'GET',
                                                  headers=self.headers)
        self.assertEqual(len(containers), 1)
        container = containers[0]
        self.assertTrue(isinstance(container, Container))
        self.assertEqual(container.name, name)

    def test_api_list_container_objects(self):
        name = utils.random_string()
        marker = utils.random_string()
        delimiter = utils.random_string()
        end_marker = utils.random_string()
        prefix = utils.random_string()
        limit = random.randint(1, 1000)
        self.api._service.list = Mock()
        self.api.list_container_objects(self.account, name, limit=limit,
                                        marker=marker,
                                        prefix=prefix, delimiter=delimiter,
                                        end_marker=end_marker,
                                        headers=self.headers)
        self.api._service.list.assert_called_once_with(self.account, name,
                                                       limit=limit,
                                                       marker=marker,
                                                       prefix=prefix,
                                                       delimiter=delimiter,
                                                       end_marker=end_marker,
                                                       headers=self.headers)

    def test_api_get_container_metadata(self):
        name = utils.random_string()
        self.api._service.get_metadata = Mock()
        self.api.get_container_metadata(self.account, name,
                                        headers=self.headers)
        self.api._service.get_metadata. \
            assert_called_once_with(self.account, name, headers=self.headers)

    def test_api_set_container_metadata(self):
        name = utils.random_string()
        key = utils.random_string()
        value = utils.random_string()
        metadata = {key: value}
        self.api._service.set_metadata = Mock()
        self.api.set_container_metadata(self.account, name, metadata,
                                        headers=self.headers)
        self.api._service.set_metadata. \
            assert_called_once_with(self.account, name, metadata,
                                    headers=self.headers)

    def test_api_delete_container_metadata(self):
        name = utils.random_string()
        key = utils.random_string()
        keys = [key]
        self.api._service.delete_metadata = Mock()
        self.api.delete_container_metadata(self.account, name, keys,
                                           headers=self.headers)
        self.api._service.delete_metadata. \
            assert_called_once_with(self.account, name, keys,
                                    headers=self.headers)

    def test_api_get_object_metadata(self):
        name = utils.random_string()
        obj = utils.random_string()
        self.api._service.get_object_metadata = Mock()
        self.api.get_object_metadata(self.account, name, obj,
                                     headers=self.headers)
        self.api._service.get_object_metadata. \
            assert_called_once_with(self.account, name, obj,
                                    headers=self.headers)

    def test_api_set_object_metadata(self):
        name = utils.random_string()
        obj = utils.random_string()
        key = utils.random_string()
        value = utils.random_string()
        metadata = {key: value}
        self.api._service.set_object_metadata = Mock()
        self.api.set_object_metadata(self.account, name, obj, metadata,
                                     clear=False, headers=self.headers)
        self.api._service.set_object_metadata. \
            assert_called_once_with(self.account, name, obj, metadata,
                                    clear=False, headers=self.headers)

    def test_api_delete_object_metadata(self):
        name = utils.random_string()
        obj = utils.random_string()
        key = utils.random_string()
        keys = [key]
        self.api._service.delete_object_metadata = Mock()
        self.api.delete_object_metadata(self.account, name, obj, keys,
                                        headers=self.headers)
        self.api._service.delete_object_metadata. \
            assert_called_once_with(self.account, name, obj, keys,
                                    headers=self.headers)

    def test_api_get_object(self):
        name = utils.random_string()
        obj = utils.random_string()
        self.api._service.get_object = Mock()
        self.api.get_object(self.account, name, obj, headers=self.headers)
        self.api._service.get_object. \
            assert_called_once_with(self.account, name, obj,
                                    headers=self.headers)

    def test_api_upload_file(self):
        name = utils.random_string()
        obj = utils.random_string()
        file_or_path = utils.random_string()
        etag = utils.random_string()
        content_type = utils.random_string()
        content_length = random.randint(1, 1000)
        key = utils.random_string()
        value = utils.random_string()
        metadata = {key: value}

        self.api._service.create_object = Mock()
        self.api.upload_file(self.account, name, file_or_path, obj_name=obj,
                             etag=etag, content_type=content_type,
                             content_length=content_length, metadata=metadata,
                             headers=self.headers, return_none=False)
        self.api._service.create_object. \
            assert_called_once_with(self.account, name,
                                    file_or_path=file_or_path,
                                    obj_name=obj, etag=etag,
                                    content_type=content_type,
                                    content_length=content_length,
                                    metadata=metadata,
                                    data=None,
                                    return_none=False,
                                    headers=self.headers)

    def test_api_store_object(self):
        name = utils.random_string()
        obj = utils.random_string()
        data = utils.random_string()
        etag = utils.random_string()
        content_type = utils.random_string()
        content_length = random.randint(1, 1000)
        key = utils.random_string()
        value = utils.random_string()
        metadata = {key: value}

        self.api._service.create_object = Mock()
        self.api.store_object(self.account, name, obj, etag=etag, data=data,
                              content_type=content_type,
                              content_length=content_length, metadata=metadata,
                              headers=self.headers, return_none=False)

        self.api._service.create_object. \
            assert_called_once_with(self.account, name, file_or_path=None,
                                    obj_name=obj, etag=etag,
                                    content_type=content_type,
                                    content_length=content_length,
                                    metadata=metadata,
                                    data=data,
                                    return_none=False,
                                    headers=self.headers)

    def test_api_create_object(self):
        name = utils.random_string()
        obj = utils.random_string()
        file_or_path = utils.random_string()
        data = utils.random_string()
        etag = utils.random_string()
        content_type = utils.random_string()
        content_length = random.randint(1, 1000)
        key = utils.random_string()
        value = utils.random_string()
        metadata = {key: value}

        self.api._service.create_object = Mock()
        self.api.create_object(self.account, name, obj_name=obj,
                               file_or_path=file_or_path, etag=etag, data=data,
                               content_type=content_type,
                               content_length=content_length, metadata=metadata,
                               headers=self.headers, return_none=False)

        self.api._service.create_object. \
            assert_called_once_with(self.account, name,
                                    file_or_path=file_or_path,
                                    obj_name=obj, etag=etag,
                                    content_type=content_type,
                                    content_length=content_length,
                                    metadata=metadata,
                                    data=data,
                                    return_none=False,
                                    headers=self.headers)

    def test_api_fetch_object(self):
        name = utils.random_string()
        obj = utils.random_string()
        size = random.randint(1, 1000)
        offset = random.randint(1, 1000)

        self.api._service.fetch_object = Mock()
        self.api.fetch_object(self.account, name, obj, size, offset,
                              with_meta=False, headers=self.headers)

        self.api._service.fetch_object. \
            assert_called_once_with(self.account, name, obj, size=size,
                                    offset=offset, with_meta=False,
                                    headers=self.headers)

    def test_api_delete(self):
        name = utils.random_string()
        self.api._service.delete = Mock()
        self.api.delete(self.account, name, headers=self.headers)
        self.api._service.delete. \
            assert_called_once_with(self.account, name, headers=self.headers)

    def test_api_delete_object(self):
        name = utils.random_string()
        obj = utils.random_string()
        self.api._service.delete_object = Mock()
        self.api.delete_object(self.account, name, obj, headers=self.headers)
        self.api._service.delete_object. \
            assert_called_once_with(self.account, name, obj,
                                    headers=self.headers)

    def test_container_list(self):
        container = self.container
        srv = container.service
        marker = utils.random_string()
        delimiter = utils.random_string()
        end_marker = utils.random_string()
        prefix = utils.random_string()
        limit = random.randint(1, 1000)
        qs = "marker=%s&max=%s&delimiter=%s&prefix=%s&end_marker=%s" % (
            marker, limit, delimiter, prefix, end_marker)
        uri = "%s/%s?%s" % (self.uri_base, utils.get_id(container), qs)
        name0 = utils.random_string()
        name1 = utils.random_string()
        resp_body = {"objects": [{"name": name0}, {"name": name1}]}
        srv.api.do_get = Mock(return_value=(None, resp_body))
        objs = srv.list(self.account, container, limit=limit, marker=marker,
                        prefix=prefix, delimiter=delimiter,
                        end_marker=end_marker, headers=None)
        srv.api.do_get.assert_called_once_with(uri, headers=None)
        self.assertEqual(len(objs), 2)
        self.assertTrue(isinstance(objs[0], StorageObject))

    def test_container_get(self):
        srv = self.container.service
        resp = fakes.FakeResponse()
        name = utils.random_string()
        cont_size = random.randint(1, 1000)
        resp.headers = {
            container_headers["size"]: cont_size
        }
        srv.api.do_head = Mock(return_value=(resp, None))
        uri = "%s/%s" % (self.uri_base, name)
        ct = srv.get(self.account, name)
        srv.api.do_head.assert_called_once_with(uri, headers=None)
        self.assertTrue(isinstance(ct, Container))
        self.assertEqual(ct.name, name)
        self.assertEqual(ct.total_size, cont_size)

    def test_container_get_not_found(self):
        srv = self.container.service
        srv.api.do_head = Mock(side_effect=exceptions.NotFound("No container"))
        name = utils.random_string()
        self.assertRaises(exceptions.NoSuchContainer, srv.get, self.account,
                          name)

    def test_container_create(self):
        container = self.container
        srv = container.service

        resp = fakes.FakeResponse()
        resp.status_code = 201
        srv.api.do_put = Mock(return_value=(resp, None))
        srv.directory.link = Mock(return_value=None)

        hresp = fakes.FakeResponse()
        name = utils.random_string()
        cont_size = random.randint(1, 1000)
        hresp.headers = {container_headers["size"]: cont_size}
        srv.api.do_head = Mock(return_value=(hresp, None))
        ct = srv.create(self.account, name)
        uri = "%s/%s" % (self.uri_base, name)

        srv.directory.link.assert_called_once_with(self.account, name, "meta2",
                                                   headers=None)
        srv.api.do_put.assert_called_once_with(uri, headers=None)
        srv.api.do_head.assert_called_once_with(uri, headers=None)

        self.assertTrue(isinstance(ct, Container))
        self.assertEqual(ct.name, name)
        self.assertEqual(ct.total_size, cont_size)

    def test_container_create_no_ref(self):
        container = self.container
        srv = container.service
        resp = fakes.FakeResponse()
        resp.status_code = 201
        srv.directory.link = Mock(side_effect=[exceptions.NotFound(""), None])
        srv.directory.create = Mock(return_value=None)
        srv.api.do_put = Mock(return_value=(resp, None))
        hresp = fakes.FakeResponse()
        name = utils.random_string()
        cont_size = random.randint(1, 1000)
        hresp.headers = {container_headers["size"]: cont_size}
        srv.api.do_head = Mock(return_value=(hresp, None))

        ct = srv.create(self.account, name)

        uri = "%s/%s" % (self.uri_base, name)

        self.assertEqual(srv.directory.link.call_count, 2)
        srv.directory.link.assert_called_with(self.account, name, "meta2",
                                              headers=None)
        srv.directory.create.assert_called_once_with(self.account, name, True,
                                                     headers=None)
        srv.api.do_put.assert_called_once_with(uri, headers=None)
        srv.api.do_head.assert_called_once_with(uri, headers=None)

        self.assertTrue(isinstance(ct, Container))
        self.assertEqual(ct.name, name)
        self.assertEqual(ct.total_size, cont_size)

    def test_container_delete(self):
        container = self.container
        srv = container.service

        resp = fakes.FakeResponse()
        resp.status_code = 204
        srv.api.do_delete = Mock(return_value=(resp, None))
        srv.directory.unlink = Mock(return_value=None)
        name = utils.random_string()
        srv.delete(self.account, name)

        uri = "%s/%s" % (self.uri_base, name)
        srv.directory.unlink.assert_called_once_with(self.account, name,
                                                     "meta2",
                                                     headers=None)
        srv.api.do_delete.assert_called_once_with(uri, headers=None)

    def test_container_delete_not_empty(self):
        container = self.container
        srv = container.service

        srv.api.do_delete = Mock(side_effect=exceptions.Conflict(""))
        srv.directory.unlink = Mock(return_value=None)
        name = utils.random_string()

        self.assertRaises(exceptions.ContainerNotEmpty, srv.delete,
                          self.account, name)

    def test_container_get_metadata(self):
        container = self.container
        srv = container.service

        name = utils.random_string()
        key = utils.random_string()
        value = utils.random_string()

        resp = fakes.FakeResponse()
        resp_body = {key: value}
        srv.api.do_post = Mock(return_value=(resp, resp_body))

        meta = srv.get_metadata(self.account, name)

        self.assertEqual(meta, resp_body)

    def test_container_set_metadata(self):
        container = self.container
        srv = container.service

        name = utils.random_string()
        key = utils.random_string()
        value = utils.random_string()
        meta = {key: value}
        resp = fakes.FakeResponse()
        srv.api.do_post = Mock(return_value=(resp, None))
        srv.set_metadata(self.account, name, meta)

        uri = "%s/%s/action" % (self.uri_base, name)
        body = {"action": "SetProperties", "args": meta}
        srv.api.do_post.assert_called_once_with(uri, body=body, headers=None)

    def test_object_get(self):
        container = self.container
        srv = container.object_service
        name = utils.random_string()
        size = random.randint(1, 1000)
        content_hash = utils.random_string()
        content_type = utils.random_string()
        resp = fakes.FakeResponse()
        resp.headers = {object_headers["name"]: name,
                        object_headers["size"]: size,
                        object_headers["hash"]: content_hash,
                        object_headers["content_type"]: content_type}
        srv.api.do_head = Mock(return_value=(resp, None))
        obj = srv.get(name)

        uri = "%s/%s/%s" % (self.uri_base, container.id, utils.quote(name))

        srv.api.do_head.assert_called_once_with(uri, headers=None)
        self.assertEqual(obj.name, name)
        self.assertEqual(obj.size, size)
        self.assertEqual(obj.hash, content_hash)
        self.assertEqual(obj.content_type, content_type)

    def test_object_create_no_data(self):
        container = self.container
        srv = container.object_service
        name = utils.random_string()
        self.assertRaises(exceptions.MissingData, srv.create)

    def test_object_create_no_name(self):
        container = self.container
        srv = container.object_service
        self.assertRaises(exceptions.MissingName, srv.create, data="x")

    def test_object_create_no_content_length(self):
        container = self.container
        srv = container.object_service
        name = utils.random_string()
        f = Mock()
        self.assertRaises(exceptions.MissingContentLength, srv.create, f,
                          obj_name=name)

    def test_object_create_missing_file(self):
        container = self.container
        srv = container.object_service
        name = utils.random_string()
        self.assertRaises(exceptions.FileNotFound, srv.create, name)

    def test_object_get_metadata(self):
        container = self.container
        srv = container.object_service
        name = utils.random_string()
        key = utils.random_string()
        value = utils.random_string()
        resp = fakes.FakeResponse()
        resp_body = {key: value}
        uri = "%s/%s/%s/action" % (self.uri_base, container.id, utils.quote(
            name))
        srv.api.do_post = Mock(return_value=(resp, resp_body))
        meta = srv.get_metadata(name)
        body = {'action': 'GetProperties', 'args': None}
        srv.api.do_post.assert_called_once_with(uri, body=body, headers=None)
        self.assertEqual(meta, resp_body)

    def test_object_set_metadata(self):
        container = self.container
        srv = container.object_service

        name = utils.random_string()
        key = utils.random_string()
        value = utils.random_string()
        meta = {key: value}
        resp = fakes.FakeResponse()
        srv.api.do_post = Mock(return_value=(resp, None))
        srv.set_metadata(name, meta)

        uri = "%s/%s/%s/action" % (self.uri_base, container.id, name)
        body = {'action': 'SetProperties', 'args': meta}
        srv.api.do_post.assert_called_once_with(uri, body=body, headers=None)

    def test_object_delete(self):
        srv = self.container.object_service
        name = utils.random_string()
        resp_body = [
            {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32},
            {"url": "http://1.2.3.4:6000/BBBB", "pos": "1", "size": 32},
            {"url": "http://1.2.3.4:6000/CCCC", "pos": "2", "size": 32}
        ]
        srv.api.do_delete = Mock(return_value=(None, resp_body))
        srv._delete = Mock()
        srv.delete(name)

        uri = "%s/%s/%s" % (self.uri_base, self.container.id, utils.quote(name))
        srv.api.do_delete.assert_called_once_with(uri, headers=None)

    def test_object_delete_not_found(self):
        srv = self.container.object_service
        name = utils.random_string()
        srv.api.do_delete = Mock(side_effect=exceptions.NotFound("No object"))
        self.assertRaises(exceptions.NoSuchObject, srv.delete, name)

    def test_object_store(self):
        srv = self.container.object_service
        name = utils.random_string()
        srv.api.do_put = Mock(return_value=(None, None))
        raw_chunks = [
            {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32},
            {"url": "http://1.2.3.4:6000/BBBB", "pos": "1", "size": 32},
            {"url": "http://1.2.3.4:6000/CCCC", "pos": "2", "size": 32}
        ]
        srv.api.do_post = Mock(return_value=(None, raw_chunks))
        srv.api.do_put = Mock(return_value=(None, None))
        with set_http_connect(201, 201, 201):
            srv.create(obj_name=name, data="x", return_none=True)

    def test_sort_chunks(self):
        srv = self.container.object_service
        raw_chunks = [
            {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32},
            {"url": "http://1.2.3.4:6000/BBBB", "pos": "0", "size": 32},
            {"url": "http://1.2.3.4:6000/CCCC", "pos": "1", "size": 32},
            {"url": "http://1.2.3.4:6000/DDDD", "pos": "1", "size": 32},
            {"url": "http://1.2.3.4:6000/EEEE", "pos": "2", "size": 32},
            {"url": "http://1.2.3.4:6000/FFFF", "pos": "2", "size": 32},
        ]
        chunks = srv._sort_chunks(raw_chunks, False)
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
        chunks = srv._sort_chunks(raw_chunks, True)
        sorted_chunks = {
            0: {
                "0":
                    {"url": "http://1.2.3.4:6000/AAAA", "pos": "0.0",
                     "size": 32},
                "1":
                    {"url": "http://1.2.3.4:6000/BBBB", "pos": "0.1",
                     "size": 32},
                "p0":
                    {"url": "http://1.2.3.4:6000/CCCC", "pos": "0.p0",
                     "size": 32}
            },
            1: {
                "0":
                    {"url": "http://1.2.3.4:6000/DDDD", "pos": "1.0",
                     "size": 32},
                "1":
                    {"url": "http://1.2.3.4:6000/EEEE", "pos": "1.1",
                     "size": 32},
                "p0":
                    {"url": "http://1.2.3.4:6000/FFFF", "pos": "1.p0",
                     "size": 32}
            }}
        self.assertEqual(chunks, sorted_chunks)

    def test_put_stream_empty(self):
        srv = self.container.object_service
        name = utils.random_string()
        chunks = {
            0: [
                {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32},
                {"url": "http://1.2.3.4:6000/BBBB", "pos": "0", "size": 32},
                {"url": "http://1.2.3.4:6000/CCCC", "pos": "0", "size": 32}
            ]
        }
        src = empty_stream()

        with set_http_connect(201, 201, 201):
            chunks, bytes_transferred, content_checksum = srv._put_stream(
                name, src, {"content_length": 0}, chunks)

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
        srv = self.container.object_service
        name = utils.random_string()
        chunks = {
            0: [
                {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32},
                {"url": "http://1.2.3.4:6000/BBBB", "pos": "0", "size": 32},
                {"url": "http://1.2.3.4:6000/CCCC", "pos": "0", "size": 32}
            ]
        }
        src = empty_stream()

        with set_http_connect(201, Exception(), Exception()):
            chunks, bytes_transferred, content_checksum = srv._put_stream(
                name, src, {"content_length": 0}, chunks)
        self.assertEqual(len(chunks), 1)
        chunk = {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 0,
                 "hash": "d41d8cd98f00b204e9800998ecf8427e"}
        self.assertEqual(chunk, chunks[0])

    def test_put_stream_connect_timeout(self):
        srv = self.container.object_service
        name = utils.random_string()
        chunks = {
            0: [
                {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32}
            ]
        }
        src = empty_stream()

        with set_http_connect(200, slow_connect=True):
            chunks, bytes_transferred, content_checksum = srv. \
                _put_stream(name, src, {"content_length": 0}, chunks)

    def test_put_stream_client_timeout(self):
        srv = self.container.object_service
        name = utils.random_string()
        chunks = {
            0: [
                {"url": "http://1.2.3.4:6000/AAAA", "pos": "0", "size": 32}
            ]
        }

        src = fakes.FakeTimeoutStream(5)

        with set_http_connect(200):
            self.assertRaises(exceptions.ClientReadTimeout, srv._put_stream,
                              name, src, {"content_length": 1}, chunks)








