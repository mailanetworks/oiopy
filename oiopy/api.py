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
from urlparse import urlparse

from eventlet import import_patched

urllib3 = import_patched('urllib3')

from oiopy import exceptions
from oiopy.http import Response


class API(object):
    """
    The base class for all API.
    """

    def __init__(self, endpoint_url):
        self.version = "v1.0"
        self._service = None
        parsed = urlparse(endpoint_url)
        self.proxyd_host = parsed.netloc
        self.endpoint_uri = "%s/%s" % (parsed.path, self.version)
        self.proxyd_pool = urllib3.HTTPConnectionPool(self.proxyd_host,
                                                      maxsize=5)

    def create(self, *args, **kwargs):
        """
        Create a new resource.
        """
        return self._service.create(*args, **kwargs)

    def get(self, resource, **kwargs):
        """
        Get a specific resource
        """
        return self._service.get(resource, **kwargs)

    def delete(self, resource, **kwargs):
        """
        Delete a specific resource.
        """
        return self._service.delete(resource, **kwargs)

    def list(self, limit=None, marker=None, **kwargs):
        """
        Get a list of resource objects.
        """
        return self._service.list(limit=limit, marker=marker, **kwargs)

    def _proxyd_request(self, method, uri, data=None, headers=None):
        """
        proxyd request
        """
        if not headers:
            headers = {}
        headers['connection'] = 'keep-alive'

        raw = self.proxyd_pool.urlopen(method, uri, body=data, headers=headers)
        resp = Response()
        resp.status_code = raw.status
        resp.reason = raw.reason
        resp.headers = raw.headers
        resp._content = raw.data
        return resp

    def _request(self, uri, method, **kwargs):
        """
        Execute the request
        """
        uri = "%s%s" % (self.endpoint_uri, uri)
        data = None
        if "body" in kwargs:
            data = json.dumps(kwargs.pop("body"))
        if "headers" in kwargs:
            headers = kwargs.pop("headers")
        else:
            headers = None
        resp = self._proxyd_request(method, uri, data, headers)
        try:
            body = resp.json()
        except ValueError:
            body = resp.content
        if resp.status_code >= 400:
            raise exceptions.from_response(resp, body)
        return resp, body

    def do_head(self, uri, **kwargs):
        return self._request(uri, "HEAD", **kwargs)

    def do_get(self, uri, **kwargs):
        return self._request(uri, "GET", **kwargs)

    def do_post(self, uri, **kwargs):
        return self._request(uri, "POST", **kwargs)

    def do_put(self, uri, **kwargs):
        return self._request(uri, "PUT", **kwargs)

    def do_delete(self, uri, **kwargs):
        return self._request(uri, "DELETE", **kwargs)

    def do_copy(self, uri, **kwargs):
        return self._request(uri, "COPY", **kwargs)
