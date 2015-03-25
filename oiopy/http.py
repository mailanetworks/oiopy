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

from eventlet.green.httplib import HTTPConnection

from oiopy.utils import InsensitiveDict


CHUNK_SIZE = 8 * 1024


class Response(object):
    def __init__(self):
        self._content = False
        self.raw = None

    @property
    def content(self):
        if self._content is False:
            self._content = str().join(
                self.iter_content(CHUNK_SIZE)) or str()
        return self._content

    @property
    def text(self):
        if self.content is None:
            return unicode('')
        content = unicode(self.content, errors='replace')
        return content

    def iter_content(self, chunk_size=1):
        def gen():
            while True:
                chunk = self.raw.read(chunk_size)
                if not chunk:
                    break
                yield chunk

        return gen()

    def json(self, **kwargs):
        return json.loads(self.text, **kwargs)


def http_connect(host, method, path, headers=None):
    conn = HTTPConnection(host)
    conn.path = path
    conn.putrequest(method, path)
    if headers:
        for header, value in headers.iteritems():
            conn.putheader(header, str(value))
    conn.endheaders()
    return conn


def http_request(host, method, path, data=None, headers=None):
    conn = HTTPConnection(host)
    if not headers:
        headers = {}
    conn.request(method, path, data, headers)
    raw_resp = conn.getresponse(True)
    resp = Response()
    resp.status_code = raw_resp.status
    resp.raw = raw_resp
    resp.reason = raw_resp.reason
    resp.headers = InsensitiveDict(raw_resp.getheaders())
    resp.content
    conn.close()
    return resp
