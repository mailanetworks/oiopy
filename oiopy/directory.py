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


from oiopy.api import API
from oiopy.resource import Resource
from oiopy.service import Service
from oiopy import exceptions
from oiopy import utils


class DirectoryAPI(API):
    """
    The directory API
    """

    def __init__(self, endpoint_url, namespace):
        self.namespace = namespace
        super(DirectoryAPI, self).__init__(endpoint_url)
        self._service = ReferenceService(self)

    def has(self, reference, headers=None):
        """
        Check if the reference exists.
        """
        return self._service.has(reference, headers=headers)

    def link(self, reference, service_type, headers=None):
        """
        Poll and associate a new service to the reference.
        """
        return self._service.link(reference, service_type, headers=headers)

    def unlink(self, reference, service_type, headers=None):
        """
        Remove an associated service to the reference.
        """
        return self._service.unlink(reference, service_type, headers=headers)

    def renew(self, reference, service_type, headers=None):
        """
        Re-poll and re-associate a set of services to the reference.
        """
        return self._service.renew(reference, service_type, headers=headers)

    def force(self, reference, service_type, services, headers=None):
        """
        Associate the specified services to the reference.
        """
        return self._service.force(reference, service_type, services,
                                   headers=headers)

    def list_services(self, reference, service_type, headers=None):
        """
        List the associated services to the reference.
        """
        return self._service.list(reference, service_type, headers=headers)

    def get_properties(self, reference, properties=None, headers=None):
        """
        Get properties for a reference.
        """
        return self._service.get_properties(reference, properties,
                                            headers=headers)

    def set_properties(self, reference, properties, headers=None):
        """
        Set properties for a reference.
        """
        return self._service.set_properties(reference, properties,
                                            headers=headers)

    def delete_properties(self, reference, properties, headers=None):
        """
        Delete properties for a reference.
        """
        return self._service.delete_properties(reference, properties,
                                               headers=headers)


class Reference(Resource):
    """
    Reference Resource
    """

    def __repr__(self):
        return "<Reference '%s'>" % self.name

    @property
    def id(self):
        return self.name

    def list(self, service_type, headers=None):
        return self.service.list(self, service_type, headers=headers)

    def unlink(self, service_type, headers=None):
        return self.service.unlink(self, service_type, headers=headers)

    def link(self, service_type, headers=None):
        return self.service.link(self, service_type, headers=headers)

    def renew(self, service_type, headers=None):
        return self.service.renew(self, service_type, headers=headers)

    def force(self, service_type, services, headers=None):
        return self.service.force(self, service_type, services, headers=headers)

    def get_properties(self, properties=None, headers=None):
        return self.service.get_properties(self, properties, headers=headers)

    def set_properties(self, properties, headers=None):
        return self.service.set_properties(self, properties, headers=headers)

    def delete_properties(self, properties, headers=None):
        return self.service.delete_properties(self, properties, headers=headers)


class ReferenceService(Service):
    def __init__(self, api):
        uri_base = '/dir/%s' % api.namespace
        super(ReferenceService, self).__init__(api, uri_base=uri_base)

    def get(self, name, headers=None):
        uri = "%s/%s" % (self.uri_base, name)
        resp, resp_body = self.api.do_get(uri, headers=headers)
        data = {"name": name}
        return Reference(self, data)

    def has(self, name, headers=None):
        uri = "%s/%s" % (self.uri_base, name)
        try:
            resp, resp_body = self.api.do_head(uri, headers=headers)
        except exceptions.NotFound:
            return False
        return True

    def create(self, name, return_none=False, headers=None):
        uri = "%s/%s" % (self.uri_base, name)
        resp, resp_body = self.api.do_put(uri, headers=headers)
        if resp.status_code in (200, 201):
            if not return_none:
                hresp, hresp_body = self.api.do_get(uri, headers=headers)
                data = {"name": name}
                return Reference(self, data)
        else:
            exceptions.from_response(resp, resp_body)

    def delete(self, reference, headers=None):
        name = utils.get_id(reference)
        uri = "%s/%s" % (self.uri_base, name)
        resp, resp_body = self.api.do_delete(uri, headers=headers)

    def list(self, reference, service_type, headers=None):
        name = utils.get_id(reference)
        uri = "%s/%s/%s" % (self.uri_base, name, service_type)
        resp, resp_body = self.api.do_get(uri, headers=headers)
        return resp_body

    def unlink(self, reference, service_type, headers=None):
        name = utils.get_id(reference)
        uri = "%s/%s/%s" % (self.uri_base, name, service_type)
        resp, resp_body = self.api.do_delete(uri, headers=headers)

    def link(self, reference, service_type, headers=None):
        name = utils.get_id(reference)
        uri = "%s/%s/%s" % (self.uri_base, name, service_type)
        resp, resp_body = self._action(uri, "Link", None, headers=headers)
        return resp_body

    def renew(self, reference, service_type, headers=None):
        name = utils.get_id(reference)
        uri = "%s/%s/%s" % (self.uri_base, name, service_type)
        resp, resp_body = self._action(uri, "Renew", None, headers=headers)
        return resp_body

    def force(self, reference, service_type, services, headers=None):
        name = utils.get_id(reference)
        uri = "%s/%s/%s" % (self.uri_base, name, service_type)
        resp, resp_body = self._action(uri, "Force", services, headers=headers)

    def get_properties(self, reference, properties=None, headers=None):
        name = utils.get_id(reference)
        uri = "%s/%s" % (self.uri_base, name)
        resp, resp_body = self._action(uri, "GetProperties", properties,
                                       headers=headers)
        return resp_body

    def set_properties(self, reference, properties, headers=None):
        name = utils.get_id(reference)
        uri = "%s/%s" % (self.uri_base, name)
        resp, resp_body = self._action(uri, "SetProperties", properties,
                                       headers=headers)

    def delete_properties(self, reference, properties, headers=None):
        name = utils.get_id(reference)
        uri = "%s/%s" % (self.uri_base, name)
        resp, resp_body = self._action(uri, "DeleteProperties", properties,
                                       headers=headers)

