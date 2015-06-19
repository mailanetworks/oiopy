import uuid
from ConfigParser import SafeConfigParser

import os
import testtools

from oiopy.object_storage import StorageAPI
from oiopy import exceptions
from oiopy import utils


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
        self.account = config.get('func_test', 'account')

    def setUp(self):
        super(TestObjectStorageFunctional, self).setUp()

        self.container_name = 'func-test-container-%s' % uuid.uuid4()
        self.container_name_2 = 'func-test-container-%s-2' % uuid.uuid4()
        self.container_name_3 = 'func-test-container-%s-3' % uuid.uuid4()

        self.object_name = "func-test-object-%s" % uuid.uuid4()
        self.object_name_2 = "func-test-object-%s-2" % uuid.uuid4()

        self.test_data = b'1337' * 10
        self.hash_data = "894A14D048263CA40300302C7A5DB67C"
        self.storage = StorageAPI(self.proxyd_uri, self.namespace)

        self.container = self.storage.create(self.account, self.container_name)
        self.container_2 = self.storage.create(self.account,
                                               self.container_name_2)
        self.container.create(obj_name=self.object_name, data=self.test_data)

    def tearDown(self):
        super(TestObjectStorageFunctional, self).tearDown()
        for obj in (self.object_name, self.object_name_2):
            try:
                self.storage.delete_object(self.account, self.container_name,
                                           obj)
            except Exception as e:
                pass

        for container in [self.container_name,
                          self.container_name_2,
                          self.container_name_3]:
            try:
                self.storage.delete(self.account, container)
            except Exception as e:
                pass

    def test_stat_container(self):
        self.container.reload()
        self.assertIsNotNone(self.container.total_size)
        self.assertTrue(self.container.namespace)
        self.assertTrue(self.container.name)

    def test_list_container(self):
        objs = self.container.list()
        self.assertEqual(len(objs), 1)
        self.assertEqual(objs[0].name, self.object_name)

    def test_create_container(self):
        container = self.storage.create(self.account, self.container_name_3)
        self.assertTrue(container)

    def test_delete_container(self):
        self.container_2.delete()
        self.assertRaises(exceptions.NoSuchContainer,
                          self.storage.get, self.account, self.container_name_2)

    def test_container_metadata(self):
        key = "user." + utils.random_string()
        value = utils.random_string()
        meta = {key: value}
        self.container.set_metadata(meta)
        rmeta = self.container.get_metadata()
        self.assertEqual(rmeta.get(key), value)
        self.container.delete_metadata([])
        rmeta = self.container.get_metadata()
        self.assertEqual(rmeta.get(key), None)
        self.assertTrue(rmeta.get("sys.m2.usage"))
        self.assertTrue(rmeta.get("sys.m2.ctime"))

    def test_object_metadata(self):
        key = utils.random_string()
        value = utils.random_string()
        meta = {key: value}
        self.container.set_object_metadata(self.object_name, meta)
        rmeta = self.container.get_object_metadata(self.object_name)
        self.assertEqual(rmeta.get(key), value)
        key2 = utils.random_string()
        value2 = utils.random_string()
        meta2 = {key2: value2}
        self.container.set_object_metadata(self.object_name, meta2, clear=True)
        rmeta = self.container.get_object_metadata(self.object_name)
        self.assertEqual(rmeta.get(key), None)
        self.assertEqual(rmeta.get(key2), value2)
        self.assertEqual(rmeta.get("name"), self.object_name)
        self.assertEqual(rmeta.get("hash"), self.hash_data)
        self.assertEqual(rmeta.get("length"), "40")
        self.assertTrue(rmeta.get("mime-type"))

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

    def test_list_account(self):
        containers, meta = self.storage.list_containers(self.account)
        self.assertEqual(len(containers), 2)
        self.assertTrue(meta)
        self.assertEqual(meta['id'], self.account)
        self.assertEqual(meta['containers'], 2)
        self.assertTrue(meta['ctime'])
        self.assertEqual(meta['metadata'], {})

    def test_stat_account(self):
        info = self.storage.get_account(self.account)
        self.assertEqual(info['id'], self.account)
        self.assertEqual(info['containers'], 2)
        self.assertTrue(info['ctime'])
        self.assertEqual(info['metadata'], {})