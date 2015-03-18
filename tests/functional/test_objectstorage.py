import uuid
from ConfigParser import SafeConfigParser

import os
import testtools

from oiopy.object_storage import StorageAPI
from oiopy import exceptions


class TestObjectStorageFunctional(testtools.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestObjectStorageFunctional, self).__init__(*args, **kwargs)
        self._load_config()

    def _load_config(self):
        default_conf_path = os.path.expanduser('~/.oio/sds/conf/test.conf')
        config_file = os.environ.get('SDS_TEST_CONFIG_FILE',
                                     default_conf_path)
        config = SafeConfigParser()
        config.read(config_file)
        self.proxyd_uri = config.get('func_test', 'proxyd_uri')
        self.namespace = config.get('func_test', 'namespace')

    def setUp(self):
        super(TestObjectStorageFunctional, self).setUp()

        self.container_name = 'func-test-container-%s' % uuid.uuid4()
        self.container_name_2 = 'func-test-container-%s-2' % uuid.uuid4()
        self.container_name_3 = 'func-test-container-%s-3' % uuid.uuid4()

        self.object_name = "func-test-object-%s" % uuid.uuid4()
        self.object_name_2 = "func-test-object-%s-2" % uuid.uuid4()

        self.test_data = b'1337' * 10
        self.storage = StorageAPI(self.proxyd_uri, self.namespace)

        self.container = self.storage.create(self.container_name)
        self.container_2 = self.storage.create(self.container_name_2)
        self.container.create(obj_name=self.object_name, data=self.test_data)

    def tearDown(self):
        super(TestObjectStorageFunctional, self).tearDown()
        for obj in (self.object_name, self.object_name_2):
            try:
                self.storage.delete_object(self.container_name, obj)
            except Exception:
                pass

        for container in [self.container_name,
                          self.container_name_2,
                          self.container_name_3]:
            try:
                self.storage.delete(container)
            except Exception:
                pass

    def test_stat_container(self):
        self.container.reload()
        self.assertIsNotNone(self.container.total_size)
        self.assertTrue(self.container.namespace)
        self.assertTrue(self.container.name)

    def test_list_container(self):
        objs = self.container.list()
        self.assertTrue(len(objs))

    def test_create_container(self):
        container = self.storage.create(self.container_name_3)
        self.assertTrue(container)

    def test_delete_container(self):
        self.container_2.delete()
        self.assertRaises(exceptions.NoSuchContainer,
                          self.storage.get, self.container_name_2)

    def test_container_metadata(self):
        self.container.set_metadata({"a": 1})
        meta = self.container.get_metadata()
        self.assertEqual(meta.get("a"), 1)

    def test_fetch_object(self):
        stream = self.container.fetch(self.object_name)
        data = "".join(stream)
        self.assertEqual(data, self.test_data)

    def test_fetch_partial_object(self):
        stream = self.container.fetch(self.object_name, size=10, offset=4)
        data = "".join(stream)
        self.assertEqual(data, self.test_data[4:10 + 4])

    def test_store_object(self):
        self.container.create(obj_name=self.object_name, data=self.test_data)
        obj = self.container.get_object(self.object_name)
        self.assertTrue(obj)

    def test_delete_object(self):
        self.container.delete_object(self.object_name)
        self.assertRaises(exceptions.NoSuchObject,
                          self.container.get_object, self.object_name)