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


import glob
import hashlib
import random
import string
from urllib import quote as _quote
from ConfigParser import SafeConfigParser

import os
import codecs
from eventlet import GreenPool


utf8_decoder = codecs.getdecoder('utf-8')
utf8_encoder = codecs.getencoder('utf-8')


def random_string(length=20):
    chars = string.ascii_letters
    return "".join(random.sample(chars, length))


def quote(value, safe='/'):
    if isinstance(value, unicode):
        (value, _len) = utf8_encoder(value, 'replace')
    (valid_utf8_str, _len) = utf8_decoder(value, 'replace')
    return _quote(valid_utf8_str.encode('utf-8'), safe)


class ContextPool(GreenPool):
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        for coroutine in list(self.coroutines_running):
            coroutine.kill()


def env(*vars, **kwargs):
    """Search for the first defined of possibly many env vars
    Returns the first environment variable defined in vars, or
    returns the default defined in kwargs.
    """
    for v in vars:
        value = os.environ.get(v, None)
        if value:
            return value
    return kwargs.get('default', '')


def name2cid(account, ref):
    h = hashlib.sha256()
    for v in [account, '\0', ref]:
        h.update(v)
    return h.hexdigest()


def load_sds_conf(ns):
    def places():
        yield '/etc/oio/sds.conf'
        for f in glob.glob('/etc/oio/sds.conf.d/*'):
            yield f
        yield os.path.expanduser('~/.oio/sds.conf')

    parser = SafeConfigParser({})
    success = parser.read(places())
    if success and parser.has_section(ns):
        return dict(parser.items(ns))
    else:
        return None


def ranges_from_http_header(val):
    if not val.startswith('bytes='):
        raise ValueError('Invalid Range value: %s' % val)
    ranges = []
    for r in val[6:].split(','):
        start, end = r.split('-', 1)
        if start:
            start = int(start)
        else:
            start = None
        if end:
            end = int(end)
            if end < 0:
                raise ValueError('Invalid byterange value: %s' % val)
            elif start is not None and end < start:
                raise ValueError('Invalid byterange value: %s' % val)
        else:
            end = None
            if start is None:
                raise ValueError('Invalid byterange value: %s' % val)
        ranges.append((start, end))
    return ranges


def http_header_from_ranges(ranges):
    s = 'bytes='
    for i, (start, end) in enumerate(ranges):
        if end:
            if end < 0:
                raise ValueError("Invalid range (%s, %s)" % (start, end))
            elif start is not None and end < start:
                raise ValueError("Invalid range (%s, %s)" % (start, end))
        else:
            if start is None:
                raise ValueError("Invalid range (%s, %s)" % (start, end))

        if start is not None:
            s += str(start)
        s += '-'

        if end is not None:
            s += str(end)
        if i < len(ranges) - 1:
            s += ','
    return s


def convert_ranges(ranges, length):
    if length is None or not ranges or ranges == []:
        return None
    result = []
    for r in ranges:
        start, end = r
        if start is None:
            if end == 0:
                # bytes=-0
                continue
            elif end > length:
                # all content must be returned
                result.append((0, length))
            else:
                result.append((length - end, length))
            continue
        if end is None:
            if start < length:
                result.append((start, length))
            else:
                # skip
                continue
        elif start < length:
            result.append((start, min(end, length)))

    return result


class HeadersDict(dict):
    def __init__(self, headers):
        self.update(headers)

    def update(self, data):
        for k, v in data:
            self[k.title()] = v

    def __setitem__(self, k, v):
        if v is None:
            self.pop(k.title(), None)
        return dict.__setitem__(self, k.title(), v)

    def get(self, k, default=None):
        return dict.get(self, k.title(), default)

    def pop(self, k, default=None):
        return dict.pop(self, k.title(), default)
