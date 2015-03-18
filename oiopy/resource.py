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


class Resource(object):
    def __init__(self, service, data):
        self.service = service
        self._data = data
        self._add_data(data)

    def _add_data(self, data):
        for (key, val) in data.iteritems():
            setattr(self, key, val)

    def __getattr__(self, item):
        try:
            return self.__dict__[item]
        except KeyError:
            raise AttributeError(
                "'%s' object has no attribute '%s'." % (self.__class__, item))

    def reload(self):
        n = self.service.get(self)
        if n:
            self._add_data(n._data)

    def delete(self):
        self.service.delete(self)