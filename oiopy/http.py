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

import re
from eventlet import patcher
from eventlet.green.httplib import HTTPConnection

requests = patcher.import_patched('requests.__init__')


def http_connect(host, method, path, headers=None):
    conn = HTTPConnection(host)
    conn.path = path
    conn.putrequest(method, path)
    if headers:
        for header, value in headers.items():
            if isinstance(value, (list, tuple)):
                for k in value:
                    conn.putheader(header, str(k))
            else:
                conn.putheader(header, str(value))
    conn.endheaders()
    return conn

_token = r'[^()<>@,;:\"/\[\]?={}\x00-\x20\x7f]+'
_ext_pattern = re.compile(
    r'(?:\s*;\s*(' + _token + r')\s*(?:=\s*(' + _token +
    r'|"(?:[^"\\]|\\.)*"))?)')


def parse_content_type(raw_content_type):
    param_list = []
    if ';' in raw_content_type:
        content_type, params = raw_content_type.split(';', 1)
        params = ';' + params
        for p in _ext_pattern.findall(params):
            k = p[0].strip()
            v = p[1].strip()
            param_list.append((k, v))
    return raw_content_type, param_list


_content_range_pattern = re.compile(r'^bytes (\d+)-(\d+)/(\d+)$')


def parse_content_range(raw_content_range):
    found = re.search(_content_range_pattern, raw_content_range)
    if not found:
        raise ValueError('invalid content-range %r' % (raw_content_range,))
    return tuple(int(x) for x in found.groups())
