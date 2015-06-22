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


from eventlet import Timeout, sleep

from oiopy.directory import DirectoryAPI
from oiopy.object_storage import ObjectStorageAPI
from oiopy.http import requests


class FakeAPI(object):
    def __init__(self, *args, **kwargs):
        pass


class FakeResponse(requests.Response):
    pass


class FakeSession(requests.Session):
    pass


def fake_http_connect(*status_iter, **kwargs):
    class FakeConn(object):
        def __init__(self, status):
            if isinstance(status, (Exception, Timeout)):
                raise status
            if isinstance(status, tuple):
                self.expect_status, self.status = status
            else:
                self.expect_status, self.status = (None, status)

        def getresponse(self):
            if isinstance(self.status, (Exception, Timeout)):
                raise self.status
            return self

        def getheaders(self):
            pass

        def read(self):
            pass

        def send(self, data):
            pass

        def close(self):
            pass

    status_iter = iter(status_iter)

    def connect(*args, **ckwargs):
        if kwargs.get("slow_connect", False):
            sleep(1)
        status = status_iter.next()

        return FakeConn(status)

    connect.status_iter = status_iter

    return connect


def fake_http_request(*status_iter, **kwargs):
    status_iter = iter(status_iter)

    def request(*args, **ckwargs):
        status = status_iter.next()
        body = None
        headers = None
        if isinstance(status, tuple):
            if len(status) is 3:
                status_code, body, headers = status
            else:
                status_code, body = status
        else:
            status_code = status
        if 'callback' in kwargs:
            kwargs['callback'](*args, **ckwargs)
        resp = FakeResponse()
        resp.status_code = status_code
        resp._content = body or ''
        resp.headers = headers
        return resp

    request.status_iter = status_iter
    return request


class FakeTimeoutStream(object):
    def __init__(self, time):
        self.time = time

    def read(self, size):
        sleep(self.time)


class FakeStorageAPI(ObjectStorageAPI):
    pass


class FakeDirectoryAPI(DirectoryAPI):
    pass
