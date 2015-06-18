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

from oiopy import exceptions
from oiopy.http import requests


class API(object):
    """
    The base class for all API.
    """

    def __init__(self, endpoint_url):
        self.version = "v2.0"
        self._service = None
        self.endpoint_uri = "%s/%s" % (endpoint_url, self.version)
        self.session = requests.Session()

    def create(self, *args, **kwargs):
        """
        Create a new resource.
        """
        return self._service.create(*args, **kwargs)

    def get(self, *args, **kwargs):
        """
        Get a specific resource
        """
        return self._service.get(*args, **kwargs)

    def delete(self, *args, **kwargs):
        """
        Delete a specific resource.
        """
        return self._service.delete(*args, **kwargs)

    def list(self, limit=None, marker=None, **kwargs):
        """
        Get a list of resource objects.
        """
        return self._service.list(limit=limit, marker=marker, **kwargs)

    def _http_request(self, method, uri, data=None, headers=None):
        """
        http request
        """
        if not headers:
            headers = {}
        headers['connection'] = 'keep-alive'

        req = requests.Request(method, uri, data=data, headers=headers)
        prepped = req.prepare()

        resp = self.session.send(prepped)

        return resp

    def _request(self, uri, method, **kwargs):
        """
        Execute the request
        """
        if not uri.startswith('http://'):
            uri = "%s%s" % (self.endpoint_uri, uri)
        data = None
        if "body" in kwargs:
            data = json.dumps(kwargs.pop("body"))
        if "headers" in kwargs:
            headers = kwargs.pop("headers")
        else:
            headers = None
        resp = self._http_request(method, uri, data, headers)
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
