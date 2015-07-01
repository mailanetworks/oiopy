# Copyright (C) 2015 OpenIO SAS

# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3.0 of the License, or (at your option) any later version.
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
# You should have received a copy of the GNU Lesser General Public
# License along with this library.

import json

from oiopy.api import API
from oiopy import exceptions
from oiopy.utils import quote


class DirectoryAPI(API):
    """
    The directory API
    """

    def __init__(self, namespace, endpoint, **kwargs):
        endpoint_v2 = '/'.join([endpoint.rstrip('/'), 'v2.0'])
        super(DirectoryAPI, self).__init__(endpoint=endpoint_v2, **kwargs)
        self.namespace = namespace

    def _make_uri(self, account, reference, srv_type=None):
        account = quote(account, '')
        reference = quote(reference)
        uri = "dir/%s/%s/%s" % (self.namespace, account, reference)
        if srv_type:
            uri += '/%s' % srv_type
        return uri

    def _action(self, uri, action, args, headers=None):
        uri = "%s/action" % uri
        body = {"action": action, "args": args}
        data = json.dumps(body)
        return self._request('POST', uri, data=data, headers=headers)

    def has(self, account, reference, headers=None):
        """
        Check if the reference exists.
        """
        uri = self._make_uri(account, reference)
        try:
            resp, resp_body = self._request('HEAD', uri, headers=headers)
        except exceptions.NotFound:
            return False
        return True

    def get(self, account, reference, headers=None):
        uri = self._make_uri(account, reference)
        resp, resp_body = self._request('GET', uri, headers=headers)
        return resp_body

    def create(self, account, reference, headers=None):
        uri = self._make_uri(account, reference)
        resp, resp_body = self._request('PUT', uri, headers=headers)
        if resp.status_code in (200, 201):
            return resp_body
        else:
            raise exceptions.from_response(resp, resp_body)

    def delete(self, account, reference, headers=None):
        uri = self._make_uri(account, reference)
        resp, resp_body = self._request('DELETE', uri, headers=headers)

    def link(self, account, reference, service_type, headers=None):
        """
        Poll and associate a new service to the reference.
        """
        uri = self._make_uri(account, reference, service_type)
        resp, resp_body = self._action(uri, 'Link', None, headers=headers)
        return resp_body

    def unlink(self, account, reference, service_type, headers=None):
        """
        Remove an associated service to the reference.
        """
        uri = self._make_uri(account, reference, service_type)
        resp, resp_body = self._request('DELETE', uri, headers=headers)

    def renew(self, account, reference, service_type, headers=None):
        """
        Re-poll and re-associate a set of services to the reference.
        """
        uri = self._make_uri(account, reference, service_type)
        resp, resp_body = self._action(uri, 'Renew', None, headers=headers)
        return resp_body

    def force(self, account, reference, service_type, services, headers=None):
        """
        Associate the specified services to the reference.
        """
        uri = self._make_uri(account, reference, service_type)
        resp, resp_body = self._action(uri, 'Force', services, headers=headers)

    def list_services(self, account, reference, service_type, headers=None):
        """
        List the associated services to the reference.
        """
        uri = self._make_uri(account, reference, service_type)
        resp, resp_body = self._request('GET', uri, headers=headers)
        return resp_body

    def get_properties(self, account, reference, properties=None,
                       headers=None):
        """
        Get properties for a reference.
        """
        uri = self._make_uri(account, reference)
        resp, resp_body = self._action(uri, 'GetProperties', properties,
                                       headers=headers)
        return resp_body

    def set_properties(self, account, reference, properties, headers=None):
        """
        Set properties for a reference.
        """
        uri = self._make_uri(account, reference)
        resp, resp_body = self._action(uri, 'SetProperties', properties,
                                       headers=headers)

    def delete_properties(self, account, reference, properties, headers=None):
        """
        Delete properties for a reference.
        """
        uri = self._make_uri(account, reference)
        resp, resp_body = self._action(uri, 'DeleteProperties', properties,
                                       headers=headers)
