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


from oiopy import utils


class Service(object):
    def __init__(self, api, resource_class=None, uri_base=None):
        self.api = api
        self.resource_class = resource_class
        self.uri_base = uri_base

    def _make_uri(self, id_or_obj):
        obj_id = utils.get_id(id_or_obj)
        return "%s/%s" % (self.uri_base, utils.quote(obj_id, ''))

    def _action(self, uri, action, args, headers=None):
        uri = "%s/action" % uri
        body = {"action": action, "args": args}
        return self.api.do_post(uri, body=body, headers=headers)