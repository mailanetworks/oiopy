import uuid
from ConfigParser import SafeConfigParser

import os
import testtools

from oiopy.directory import DirectoryAPI
from oiopy import exceptions


class TestDirectoryFunctional(testtools.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDirectoryFunctional, self).__init__(*args, **kwargs)
        self._load_config()

    def _load_config(self):
        default_conf_path = os.path.expanduser('~/.oio/sds/conf/test.conf')
        config_file = os.environ.get('SDS_TEST_CONFIG_FILE',
                                     default_conf_path)
        config = SafeConfigParser()
        config.read(config_file)
        self.proxyd_uri = config.get('func_test', 'proxyd_uri')
        self.namespace = config.get('func_test', 'namespace')
        self.account = config.get('func_test', 'account')

    def setUp(self):
        super(TestDirectoryFunctional, self).setUp()

        self.reference_name = 'func-test-reference-%s' % uuid.uuid4()
        self.reference_name_2 = 'func-test-reference-%s-2' % uuid.uuid4()
        self.reference_name_3 = 'func-test-reference-%s-3' % uuid.uuid4()

        self.directory = DirectoryAPI(self.proxyd_uri, self.namespace)

        self.directory.create(self.account, self.reference_name)
        self.directory.create(self.account, self.reference_name_2)
        self.directory.link(self.account, self.reference_name, "meta2")

    def tearDown(self):
        super(TestDirectoryFunctional, self).tearDown()
        for ref in (self.reference_name,
                    self.reference_name_2,
                    self.reference_name_3):
            try:
                self.directory.delete(self.account, ref)
            except exceptions.ClientException:
                pass

    def test_has_reference(self):
        self.assertTrue(self.directory.has(self.account, self.reference_name))
        self.assertFalse(
            self.directory.has(self.account, self.reference_name_3))

    def test_stat_reference(self):
        reference = self.directory.get(self.account, self.reference_name)
        self.assertTrue(reference.name)

    def test_list_services_reference(self):
        services = self.directory.list_services(self.account,
                                                self.reference_name, "meta2")
        self.assertTrue(len(services))

    def test_create_reference(self):
        container = self.directory.create(self.account, self.reference_name_3)
        self.assertTrue(container)

    def test_delete_reference(self):
        self.directory.delete(self.account, self.reference_name_2)
        self.assertRaises(exceptions.NotFound,
                          self.directory.get, self.account,
                          self.reference_name_2)

    def test_link_reference(self):
        self.directory.link(self.account, self.reference_name_2, "meta2")
        services = self.directory.list_services(self.account,
                                                self.reference_name_2, "meta2")
        self.assertTrue(len(services))

    def test_unlink_reference(self):
        self.directory.unlink(self.account, self.reference_name, "meta2")
        services = self.directory.list_services(self.account,
                                                self.reference_name, "meta2")
        self.assertFalse(len(services))

    def test_renew_reference(self):
        services = self.directory.renew(self.account, self.reference_name,
                                        "meta2")
        self.assertTrue(len(services))

    def test_force_reference(self):
        services = {'seq': 1, 'type': 'meta2', 'host': '127.0.0.1:7000'}
        self.directory.force(self.account, self.reference_name, "meta2",
                             services)

    def test_properties(self):
        self.directory.set_properties(self.account, self.reference_name,
                                      {'data': 'something'})
        props = self.directory.get_properties(self.account, self.reference_name)
        self.assertTrue(props)
        self.assertEqual('something', props.get('data'))

        self.directory.delete_properties(self.account, self.reference_name,
                                         ['data'])
        props = self.directory.get_properties(self.account, self.reference_name,
                                              ['data'])
        self.assertFalse(len(props))