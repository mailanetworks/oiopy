import unittest
import json

from mock import MagicMock as Mock

from oiopy import fakes
from oiopy import utils
from oiopy import exceptions


class DirectoryTest(unittest.TestCase):
    def setUp(self):
        self.endpoint = "http://1.2.3.4:8000"
        self.api = fakes.FakeDirectoryAPI("NS", self.endpoint)
        self.account = "AUTH_test"
        self.headers = {"x-req-id": utils.random_string()}
        self.uri_base = "dir/NS/%s" % self.account

    def test_get(self):
        api = self.api
        resp = fakes.FakeResponse()
        name = utils.random_string()
        api._request = Mock(return_value=(resp, None))
        uri = "%s/%s" % (self.uri_base, name)
        api.get(self.account, name)
        api._request.assert_called_once_with('GET', uri, headers=None)

    def test_has(self):
        api = self.api
        resp = fakes.FakeResponse()
        name = utils.random_string()
        api._request = Mock(return_value=(resp, None))
        uri = "%s/%s" % (self.uri_base, name)
        self.assertTrue(api.has(self.account, name))
        api._request.assert_called_once_with('HEAD', uri, headers=None)

    def test_has_not_found(self):
        api = self.api
        name = utils.random_string()
        api._request = Mock(side_effect=exceptions.NotFound("No reference"))
        self.assertFalse(api.has(self.account, name))

    def test_create(self):
        api = self.api
        name = utils.random_string()
        resp = fakes.FakeResponse()
        resp.status_code = 201
        api._request = Mock(return_value=(resp, None))
        api.create(self.account, name)
        uri = "%s/%s" % (self.uri_base, name)

        api._request.assert_called_with('PUT', uri, headers=None)

    def test_create_already_exists(self):
        api = self.api
        name = utils.random_string()
        resp = fakes.FakeResponse()
        resp.status_code = 200
        api._request = Mock(return_value=(resp, None))
        api.create(self.account, name)
        uri = "%s/%s" % (self.uri_base, name)

        api._request.assert_called_once_with('PUT', uri, headers=None)

    def test_create_error(self):
        api = self.api
        name = utils.random_string()
        resp = fakes.FakeResponse()
        resp.status_code = 300
        api._request = Mock(return_value=(resp, None))

        self.assertRaises(exceptions.ClientException, api.create, self.account,
                          name)

    def test_delete(self):
        api = self.api
        name = utils.random_string()
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        uri = "%s/%s" % (self.uri_base, name)
        api.delete(self.account, name)
        api._request.assert_called_once_with('DELETE', uri, headers=None)

    def test_list(self):
        api = self.api
        name = utils.random_string()
        service_type = utils.random_string()
        resp = fakes.FakeResponse()
        resp_body = [{"seq": 1,
                      "type": service_type,
                      "host": "127.0.0.1:6000",
                      "args": ""}]

        api._request = Mock(return_value=(resp, resp_body))
        uri = "%s/%s/%s" % (self.uri_base, name, service_type)
        l = api.list_services(self.account, name, service_type)
        api._request.assert_called_once_with('GET', uri, headers=None)
        self.assertEqual(l, resp_body)

    def test_unlink(self):
        api = self.api
        name = utils.random_string()
        service_type = utils.random_string()
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        uri = "%s/%s/%s" % (self.uri_base, name, service_type)
        api.unlink(self.account, name, service_type)
        api._request.assert_called_once_with('DELETE', uri, headers=None)

    def test_link(self):
        api = self.api
        name = utils.random_string()
        service_type = utils.random_string()
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        uri = "%s/%s/%s/action" % (self.uri_base, name, service_type)
        api.link(self.account, name, service_type)
        data = json.dumps({'action': 'Link', 'args': None})
        api._request.assert_called_once_with('POST', uri, data=data,
                                             headers=None)

    def test_renew(self):
        api = self.api
        name = utils.random_string()
        service_type = utils.random_string()
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        uri = "%s/%s/%s/action" % (
            self.uri_base, name, service_type)
        api.renew(self.account, name, service_type)
        data = json.dumps({'action': 'Renew', 'args': None})
        api._request. \
            assert_called_once_with('POST', uri, data=data, headers=None)

    def test_force(self):
        api = self.api
        name = utils.random_string()
        service_type = utils.random_string()
        services = {'seq': 1, 'type': service_type, 'host': '127.0.0.1:8000'}
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        uri = "%s/%s/%s/action" % (
            self.uri_base, name, service_type)
        api.force(self.account, name, service_type, services)
        data = json.dumps({'action': 'Force', 'args': services})
        api._request. \
            assert_called_once_with('POST', uri, data=data, headers=None)

    def test_get_properties(self):
        api = self.api
        name = utils.random_string()
        properties = [utils.random_string()]
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        uri = "%s/%s/action" % (self.uri_base, name)
        api.get_properties(self.account, name, properties)
        data = json.dumps({'action': 'GetProperties', 'args': properties})
        api._request. \
            assert_called_once_with('POST', uri, data=data, headers=None)

    def test_set_properties(self):
        api = self.api
        name = utils.random_string()
        properties = {utils.random_string(): utils.random_string()}
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        uri = "%s/%s/action" % (self.uri_base, name)
        api.set_properties(self.account, name, properties)
        data = json.dumps({'action': 'SetProperties', 'args': properties})
        api._request. \
            assert_called_once_with('POST', uri, data=data, headers=None)

    def test_delete_properties(self):
        api = self.api
        name = utils.random_string()
        properties = [utils.random_string()]
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        uri = "%s/%s/action" % (self.uri_base, name)
        api.delete_properties(self.account, name, properties)
        data = json.dumps({'action': 'DeleteProperties', 'args': properties})
        api._request. \
            assert_called_once_with('POST', uri, data=data, headers=None)
