import unittest

from mock import MagicMock as Mock

from oiopy.directory import Reference
from oiopy import fakes
from oiopy import utils
from oiopy import exceptions


class DirectoryTest(unittest.TestCase):
    def setUp(self):
        self.api = fakes.FakeDirectoryAPI("http://1.2.3.4:8000", "NS")
        self.account = "AUTH_test"
        self.reference = self.api.create(self.account, "fake")
        self.headers = {"x-req-id": utils.random_string()}
        self.uri_base = "/dir/NS/%s" % self.account

    def test_ref_id(self):
        self.assertEqual(self.reference.id, "fake")

    def test_ref_repr(self):
        self.assertEqual(self.reference.__repr__(), "<Reference 'fake'>")

    def test_ref_list(self):
        service_type = utils.random_string()
        self.reference.service.list = Mock()
        self.reference.list(service_type)
        self.reference.service.list.assert_called_once_with(self.account,
                                                            self.reference,
                                                            service_type,
                                                            headers=None)

    def test_ref_unlink(self):
        service_type = utils.random_string()
        self.reference.service.unlink = Mock()
        self.reference.unlink(service_type)
        self.reference.service.unlink.assert_called_once_with(self.account,
                                                              self.reference,
                                                              service_type,
                                                              headers=None)

    def test_ref_link(self):
        service_type = utils.random_string()
        self.reference.service.link = Mock()
        self.reference.link(service_type)
        self.reference.service.link.assert_called_once_with(self.account,
                                                            self.reference,
                                                            service_type,
                                                            headers=None)

    def test_ref_renew(self):
        service_type = utils.random_string()
        self.reference.service.renew = Mock()
        self.reference.renew(service_type)
        self.reference.service.renew.assert_called_once_with(self.account,
                                                             self.reference,
                                                             service_type,
                                                             headers=None)

    def test_ref_force(self):
        service_type = utils.random_string()
        services = {'seq': 1, 'type': service_type, 'host': '127.0.0.1:8000'}
        self.reference.service.force = Mock()
        self.reference.force(service_type, services)
        self.reference.service.force.assert_called_once_with(self.account,
                                                             self.reference,
                                                             service_type,
                                                             services,
                                                             headers=None)

    def test_ref_get_properties(self):
        properties = [utils.random_string()]
        self.reference.service.get_properties = Mock()
        self.reference.get_properties(properties)
        self.reference.service.get_properties. \
            assert_called_once_with(self.account,
                                    self.reference,
                                    properties,
                                    headers=None)

    def test_ref_set_properties(self):
        properties = {utils.random_string(): utils.random_string()}
        self.reference.service.set_properties = Mock()
        self.reference.set_properties(properties)
        self.reference.service.set_properties. \
            assert_called_once_with(self.account,
                                    self.reference,
                                    properties,
                                    headers=None)

    def test_ref_delete_properties(self):
        properties = [utils.random_string()]
        self.reference.service.delete_properties = Mock()
        self.reference.delete_properties(properties)
        self.reference.service.delete_properties. \
            assert_called_once_with(self.account,
                                    self.reference,
                                    properties,
                                    headers=None)

    def test_api_has(self):
        name = utils.random_string()
        self.api._service.has = Mock()
        self.api.has(self.account, name, headers=self.headers)
        self.api._service.has.assert_called_once_with(self.account, name,
                                                      headers=self.headers)

    def test_api_link(self):
        name = utils.random_string()
        service_type = utils.random_string()
        self.api._service.link = Mock()
        self.api.link(self.account, name, service_type, headers=self.headers)
        self.api._service.link.assert_called_once_with(self.account, name,
                                                       service_type,
                                                       headers=self.headers)

    def test_api_unlink(self):
        name = utils.random_string()
        service_type = utils.random_string()
        self.api._service.unlink = Mock()
        self.api.unlink(self.account, name, service_type, headers=self.headers)
        self.api._service.unlink.assert_called_once_with(self.account, name,
                                                         service_type,
                                                         headers=self.headers)

    def test_api_renew(self):
        name = utils.random_string()
        service_type = utils.random_string()
        self.api._service.renew = Mock()
        self.api.renew(self.account, name, service_type, headers=self.headers)
        self.api._service.renew.assert_called_once_with(self.account, name,
                                                        service_type,
                                                        headers=self.headers)

    def test_api_force(self):
        name = utils.random_string()
        service_type = utils.random_string()
        services = {'seq': 1, 'type': 'meta2', 'host': '127.0.0.1:7000'}
        self.api._service.force = Mock()
        self.api.force(self.account, name, service_type, services,
                       headers=self.headers)
        self.api._service.force. \
            assert_called_once_with(self.account, name, service_type, services,
                                    headers=self.headers)

    def test_api_list_services(self):
        name = utils.random_string()
        service_type = utils.random_string()
        self.api._service.list = Mock()
        self.api.list_services(self.account, name, service_type,
                               headers=self.headers)
        self.api._service.list. \
            assert_called_once_with(self.account, name, service_type,
                                    headers=self.headers)

    def test_api_get_properties(self):
        name = utils.random_string()
        properties = [utils.random_string(), utils.random_string()]
        self.api._service.get_properties = Mock()
        self.api.get_properties(self.account, name, properties,
                                headers=self.headers)
        self.api._service.get_properties. \
            assert_called_once_with(self.account, name, properties,
                                    headers=self.headers)

    def test_api_set_properties(self):
        name = utils.random_string()
        properties = {utils.random_string(): utils.random_string()}
        self.api._service.set_properties = Mock()
        self.api.set_properties(self.account, name, properties,
                                headers=self.headers)
        self.api._service.set_properties. \
            assert_called_once_with(self.account, name, properties,
                                    headers=self.headers)

    def test_api_delete_properties(self):
        name = utils.random_string()
        properties = [utils.random_string(), utils.random_string()]
        self.api._service.delete_properties = Mock()
        self.api.delete_properties(self.account, name, properties,
                                   headers=self.headers)
        self.api._service.delete_properties. \
            assert_called_once_with(self.account, name, properties,
                                    headers=self.headers)

    def test_get(self):
        srv = self.reference.service
        resp = fakes.FakeResponse()
        name = utils.random_string()
        srv.api.do_get = Mock(return_value=(resp, None))
        uri = "%s/%s" % (self.uri_base, name)
        srv.get(self.account, name)
        srv.api.do_get.assert_called_once_with(uri, headers=None)

    def test_has(self):
        srv = self.reference.service
        resp = fakes.FakeResponse()
        name = utils.random_string()
        srv.api.do_head = Mock(return_value=(resp, None))
        uri = "%s/%s" % (self.uri_base, name)
        self.assertTrue(srv.has(self.account, name))
        srv.api.do_head.assert_called_once_with(uri, headers=None)

    def test_has_not_found(self):
        srv = self.reference.service
        name = utils.random_string()
        srv.api.do_head = Mock(side_effect=exceptions.NotFound("No reference"))
        self.assertFalse(srv.has(self.account, name))

    def test_create(self):
        srv = self.reference.service
        name = utils.random_string()
        resp = fakes.FakeResponse()
        resp.status_code = 201
        srv.api.do_put = Mock(return_value=(resp, None))
        srv.api.do_get = Mock(return_value=(resp, None))
        ref = srv.create(self.account, name)
        uri = "%s/%s" % (self.uri_base, name)

        srv.api.do_put.assert_called_once_with(uri, headers=None)
        srv.api.do_get.assert_called_once_with(uri, headers=None)

        self.assertTrue(isinstance(ref, Reference))
        self.assertEqual(ref.name, name)

    def test_create_return_none(self):
        srv = self.reference.service
        name = utils.random_string()
        resp = fakes.FakeResponse()
        resp.status_code = 200
        srv.api.do_put = Mock(return_value=(resp, None))
        ref = srv.create(self.account, name, return_none=True)
        uri = "%s/%s" % (self.uri_base, name)
        srv.api.do_put.assert_called_once_with(uri, headers=None)
        self.assertIsNone(ref)

    def test_create_already_exists(self):
        srv = self.reference.service
        name = utils.random_string()
        resp = fakes.FakeResponse()
        resp.status_code = 200
        srv.api.do_put = Mock(return_value=(resp, None))
        srv.api.do_get = Mock(return_value=(resp, None))
        ref = srv.create(self.account, name)
        uri = "%s/%s" % (self.uri_base, name)

        srv.api.do_put.assert_called_once_with(uri, headers=None)
        srv.api.do_get.assert_called_once_with(uri, headers=None)

        self.assertTrue(isinstance(ref, Reference))
        self.assertEqual(ref.name, name)

    def test_create_error(self):
        srv = self.reference.service
        name = utils.random_string()
        resp = fakes.FakeResponse()
        resp.status_code = 300
        srv.api.do_put = Mock(return_value=(resp, None))

        self.assertRaises(exceptions.ClientException, srv.create, self.account,
                          name)

    def test_delete(self):
        srv = self.reference.service
        name = utils.random_string()
        resp = fakes.FakeResponse()
        srv.api.do_delete = Mock(return_value=(resp, None))
        uri = "%s/%s" % (self.uri_base, name)
        srv.delete(self.account, name)
        srv.api.do_delete.assert_called_once_with(uri, headers=None)

    def test_list(self):
        srv = self.reference.service
        name = utils.random_string()
        service_type = utils.random_string()
        resp = fakes.FakeResponse()
        resp_body = [{"seq": 1,
                      "type": service_type,
                      "host": "127.0.0.1:6000",
                      "args": ""}]

        srv.api.do_get = Mock(return_value=(resp, resp_body))
        uri = "%s/%s/%s" % (self.uri_base, name, service_type)
        l = srv.list(self.account, name, service_type)
        srv.api.do_get.assert_called_once_with(uri, headers=None)
        self.assertEqual(l, resp_body)

    def test_unlink(self):
        srv = self.reference.service
        name = utils.random_string()
        service_type = utils.random_string()
        resp = fakes.FakeResponse()
        srv.api.do_delete = Mock(return_value=(resp, None))
        uri = "%s/%s/%s" % (self.uri_base, name, service_type)
        srv.unlink(self.account, name, service_type)
        srv.api.do_delete.assert_called_once_with(uri, headers=None)

    def test_link(self):
        srv = self.reference.service
        name = utils.random_string()
        service_type = utils.random_string()
        resp = fakes.FakeResponse()
        srv.api.do_post = Mock(return_value=(resp, None))
        uri = "%s/%s/%s/action" % (self.uri_base, name, service_type)
        srv.link(self.account, name, service_type)
        srv.api.do_post. \
            assert_called_once_with(uri, body={'action': 'Link', 'args': None},
                                    headers=None)

    def test_renew(self):
        srv = self.reference.service
        name = utils.random_string()
        service_type = utils.random_string()
        resp = fakes.FakeResponse()
        srv.api.do_post = Mock(return_value=(resp, None))
        uri = "%s/%s/%s/action" % (
            self.uri_base, name, service_type)
        srv.renew(self.account, name, service_type)
        srv.api.do_post. \
            assert_called_once_with(uri, body={'action': 'Renew', 'args': None},
                                    headers=None)

    def test_force(self):
        srv = self.reference.service
        name = utils.random_string()
        service_type = utils.random_string()
        services = {'seq': 1, 'type': service_type, 'host': '127.0.0.1:8000'}
        resp = fakes.FakeResponse()
        srv.api.do_post = Mock(return_value=(resp, None))
        uri = "%s/%s/%s/action" % (
            self.uri_base, name, service_type)
        srv.force(self.account, name, service_type, services)
        srv.api.do_post. \
            assert_called_once_with(uri,
                                    body={'action': 'Force', 'args': services},
                                    headers=None)

    def test_get_properties(self):
        srv = self.reference.service
        name = utils.random_string()
        properties = [utils.random_string()]
        resp = fakes.FakeResponse()
        srv.api.do_post = Mock(return_value=(resp, None))
        uri = "%s/%s/action" % (self.uri_base, name)
        srv.get_properties(self.account, name, properties)
        srv.api.do_post. \
            assert_called_once_with(uri,
                                    body={'action': 'GetProperties',
                                          'args': properties},
                                    headers=None)

    def test_set_properties(self):
        srv = self.reference.service
        name = utils.random_string()
        properties = {utils.random_string(): utils.random_string()}
        resp = fakes.FakeResponse()
        srv.api.do_post = Mock(return_value=(resp, None))
        uri = "%s/%s/action" % (self.uri_base, name)
        srv.set_properties(self.account, name, properties)
        srv.api.do_post. \
            assert_called_once_with(uri,
                                    body={'action': 'SetProperties',
                                          'args': properties},
                                    headers=None)

    def test_delete_properties(self):
        srv = self.reference.service
        name = utils.random_string()
        properties = [utils.random_string()]
        resp = fakes.FakeResponse()
        srv.api.do_post = Mock(return_value=(resp, None))
        uri = "%s/%s/action" % (self.uri_base, name)
        srv.delete_properties(self.account, name, properties)
        srv.api.do_post. \
            assert_called_once_with(uri,
                                    body={'action': 'DeleteProperties',
                                          'args': properties},
                                    headers=None)