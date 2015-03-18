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


import random
import string
from urllib import quote as _quote
import collections

import codecs

from eventlet import GreenPool


utf8_decoder = codecs.getdecoder('utf-8')
utf8_encoder = codecs.getencoder('utf-8')


def get_id(id_or_obj):
    if isinstance(id_or_obj, basestring):
        return id_or_obj
    try:
        return id_or_obj.id
    except AttributeError:
        return id_or_obj


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


class InsensitiveDict(collections.MutableMapping):
    def __init__(self, data=None, **kwargs):
        self._data = dict()
        if data is None:
            data = {}
        self.update(data, **kwargs)

    def __setitem__(self, key, value):
        self._data[key.lower()] = (key, value)

    def __getitem__(self, item):
        return self._data[item.lower()][1]

    def __delitem__(self, key):
        del self._data[key.key.lower()]

    def __iter__(self):
        return (k for k, _ in self._data.values())

    def __len__(self):
        return len(self._data)

    def lower_items(self):
        return ((lowkey, val[1]) for (lowkey, val) in self._data.items())

    def __eq__(self, other):
        if isinstance(other, collections.Mapping):
            other = InsensitiveDict(object)
        else:
            return NotImplemented
        return dict(self.lower_items()) == dict(other.lower_items())

    def copy(self):
        return InsensitiveDict(self._data.values())

    def __repr__(self):
        return str(dict(self.items()))
