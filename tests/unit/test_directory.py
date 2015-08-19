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
        self.uri_base = "NS"

    def test_get(self):
        api = self.api
        resp = fakes.FakeResponse()
        name = utils.random_string()
        api._request = Mock(return_value=(resp, None))
        uri = "%s/reference/show" % self.uri_base
        params = {'acct': self.account, 'ref': name}
        api.get(self.account, name)
        api._request.assert_called_once_with(
            'GET', uri, params=params, headers=None)

    def test_has(self):
        api = self.api
        resp = fakes.FakeResponse()
        name = utils.random_string()
        api._request = Mock(return_value=(resp, None))
        uri = "%s/reference/has" % self.uri_base
        params = {'acct': self.account, 'ref': name}
        self.assertTrue(api.has(self.account, name))
        api._request.assert_called_once_with(
            'GET', uri, params=params, headers=None)

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
        uri = "%s/reference/create" % self.uri_base
        params = {'acct': self.account, 'ref': name}

        api._request.assert_called_with(
            'POST', uri, params=params, headers=None)

    def test_create_already_exists(self):
        api = self.api
        name = utils.random_string()
        resp = fakes.FakeResponse()
        resp.status_code = 200
        api._request = Mock(return_value=(resp, None))
        api.create(self.account, name)
        uri = "%s/reference/create" % self.uri_base
        params = {'acct': self.account, 'ref': name}

        api._request.assert_called_once_with(
            'POST', uri, params=params, headers=None)

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
        api.delete(self.account, name)
        uri = "%s/reference/destroy" % self.uri_base
        params = {'acct': self.account, 'ref': name}
        api._request.assert_called_once_with(
            'POST', uri, params=params, headers=None)

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
        l = api.list_services(self.account, name, service_type)
        uri = "%s/reference/show" % self.uri_base
        params = {'acct': self.account, 'ref': name,
                  'type': service_type}
        api._request.assert_called_once_with(
            'GET', uri, params=params, headers=None)
        self.assertEqual(l, resp_body)

    def test_unlink(self):
        api = self.api
        name = utils.random_string()
        service_type = utils.random_string()
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        api.unlink(self.account, name, service_type)
        uri = "%s/reference/unlink" % self.uri_base
        params = {'acct': self.account, 'ref': name,
                  'type': service_type}
        api._request.assert_called_once_with(
            'POST', uri, params=params, headers=None)

    def test_link(self):
        api = self.api
        name = utils.random_string()
        service_type = utils.random_string()
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        api.link(self.account, name, service_type)
        uri = "%s/reference/link" % self.uri_base
        params = {'acct': self.account, 'ref': name,
                  'type': service_type}
        api._request.assert_called_once_with(
            'POST', uri, params=params, headers=None)

    def test_renew(self):
        api = self.api
        name = utils.random_string()
        service_type = utils.random_string()
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        api.renew(self.account, name, service_type)
        uri = "%s/reference/renew" % self.uri_base
        params = {'acct': self.account, 'ref': name,
                  'type': service_type}
        api._request.assert_called_once_with(
            'POST', uri, params=params, headers=None)

    def test_force(self):
        api = self.api
        name = utils.random_string()
        service_type = utils.random_string()
        services = {'seq': 1, 'type': service_type, 'host': '127.0.0.1:8000'}
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        api.force(self.account, name, service_type, services)
        uri = "%s/reference/force" % self.uri_base
        params = {'acct': self.account, 'ref': name,
                  'type': service_type}
        data = json.dumps(services)
        api._request.assert_called_once_with(
            'POST', uri, data=data, params=params, headers=None)

    def test_get_properties(self):
        api = self.api
        name = utils.random_string()
        properties = [utils.random_string()]
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        api.get_properties(self.account, name, properties)
        uri = "%s/reference/get_properties" % self.uri_base
        params = {'acct': self.account, 'ref': name}
        data = json.dumps(properties)
        api._request.assert_called_once_with(
            'POST', uri, data=data, params=params, headers=None)

    def test_set_properties(self):
        api = self.api
        name = utils.random_string()
        properties = {utils.random_string(): utils.random_string()}
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        api.set_properties(self.account, name, properties)
        uri = "%s/reference/set_properties" % self.uri_base
        params = {'acct': self.account, 'ref': name}
        data = json.dumps(properties)
        api._request.assert_called_once_with(
            'POST', uri, data=data, params=params, headers=None)

    def test_delete_properties(self):
        api = self.api
        name = utils.random_string()
        properties = [utils.random_string()]
        resp = fakes.FakeResponse()
        api._request = Mock(return_value=(resp, None))
        api.del_properties(self.account, name, properties)
        uri = "%s/reference/del_properties" % self.uri_base
        params = {'acct': self.account, 'ref': name}
        data = json.dumps(properties)
        api._request.assert_called_once_with(
            'POST', uri, data=data, params=params, headers=None)
