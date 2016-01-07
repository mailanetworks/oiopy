import uuid
import os
import requests
import hashlib

from testtools.testcase import ExpectedException

from oiopy.exceptions import OioException
from tests.functional.utils import FunctionalTestCase
from oiopy.object_storage import ObjectStorageAPI
from oiopy import exceptions
from oiopy import utils


def md5_data(data):
    checksum = hashlib.md5()
    checksum.update(data)
    return checksum.hexdigest().upper()


class TestObjectStorageFunctional(FunctionalTestCase):
    def setUp(self):
        super(TestObjectStorageFunctional, self).setUp()

        self.session = requests.session()

        self.container_name = 'func-test-container-%s' % uuid.uuid4()
        self.container_name_2 = 'func-test-container-%s-2' % uuid.uuid4()
        self.container_name_3 = 'func-test-container-%s-3' % uuid.uuid4()

        self.object_name = "func-test-object-%s" % uuid.uuid4()
        self.object_name_2 = "func-test-object-%s-2" % uuid.uuid4()

        self.test_data = b'1337' * 10
        self.hash_data = "894A14D048263CA40300302C7A5DB67C"
        self.storage = ObjectStorageAPI(self.namespace, self.proxyd_uri)

        self.storage.container_create(self.account, self.container_name)
        self.storage.container_create(self.account, self.container_name_2)
        self.storage.object_create(self.account, self.container_name,
                                   obj_name=self.object_name,
                                   data=self.test_data)

    def tearDown(self):
        super(TestObjectStorageFunctional, self).tearDown()
        for obj in (self.object_name, self.object_name_2):
            try:
                self.storage.object_delete(self.account, self.container_name,
                                           obj)
            except Exception:
                pass

        for container in [self.container_name,
                          self.container_name_2,
                          self.container_name_3]:
            try:
                self.storage.container_delete(self.account, container)
            except Exception:
                pass

    def test_show_container(self):
        info = self.storage.container_show(self.account, self.container_name)
        self.assertTrue(info)

    def test_object_list(self):
        l = self.storage.object_list(self.account, self.container_name)
        self.assertEqual(len(l['objects']), 1)
        obj = l['objects'][0]
        self.assertEqual(obj['name'], self.object_name)
        self.assertEqual(obj['hash'], '894A14D048263CA40300302C7A5DB67C')
        self.assertEqual(obj['size'], 40)
        self.assertEqual(obj['ver'], 0)
        self.assertEqual(obj['deleted'], False)
        self.assertTrue(obj['ctime'])
        self.assertTrue(obj['system_metadata'])
        self.assertTrue(obj['policy'])

    def test_create_container(self):
        self.storage.container_create(self.account,
                                      self.container_name_3)

    def test_delete_container(self):
        self.storage.container_delete(self.account, self.container_name_2)
        self.assertRaises(exceptions.NoSuchContainer,
                          self.storage.container_show, self.account,
                          self.container_name_2)

    def test_container_metadata(self):
        key = "user." + utils.random_string()
        key2 = "user." + utils.random_string()
        value = utils.random_string()

        meta = {key: value}
        self.storage.container_update(self.account, self.container_name, meta)
        rmeta = self.storage.container_show(self.account, self.container_name)
        self.assertEqual(rmeta.get(key), value)
        self.storage.container_update(self.account, self.container_name,
                                      {key2: value},
                                      True)
        rmeta = self.storage.container_show(self.account, self.container_name)
        self.assertEqual(rmeta.get(key), None)
        self.assertEqual(rmeta.get(key2), value)
        self.assertTrue(rmeta.get("sys.m2.usage"))
        self.assertTrue(rmeta.get("sys.m2.ctime"))

    def test_object_metadata(self):
        key = utils.random_string()
        value = utils.random_string()
        meta = {key: value}
        self.storage.object_update(self.account, self.container_name,
                                   self.object_name, meta)
        rmeta = self.storage.object_show(self.account, self.container_name,
                                         self.object_name)
        self.assertEqual(rmeta['properties'].get(key), value)
        key2 = utils.random_string()
        value2 = utils.random_string()
        meta2 = {key2: value2}
        self.storage.object_update(self.account, self.container_name,
                                   self.object_name, meta2, clear=True)
        rmeta = self.storage.object_show(self.account, self.container_name,
                                         self.object_name)
        self.assertEqual(rmeta['properties'].get(key), None)
        self.assertEqual(rmeta['properties'].get(key2), value2)
        self.assertEqual(rmeta.get("name"), self.object_name)
        self.assertEqual(rmeta.get("hash"), self.hash_data)
        self.assertEqual(rmeta.get("length"), "40")
        self.assertTrue(rmeta.get("mime-type"))

    def test_fetch_object(self):
        meta, stream = self.storage.object_fetch(self.account,
                                                 self.container_name,
                                                 self.object_name)
        data = "".join(stream)
        self.assertEqual(data, self.test_data)

    def test_fetch_partial_object(self):
        meta, stream = self.storage.object_fetch(self.account,
                                                 self.container_name,
                                                 self.object_name, size=10,
                                                 offset=4)
        data = "".join(stream)
        self.assertEqual(data, self.test_data[4:10 + 4])

    def test_store_object(self):
        self.storage.object_create(self.account,
                                   self.container_name,
                                   obj_name=self.object_name,
                                   data=self.test_data)
        obj = self.storage.object_show(self.account, self.container_name,
                                       self.object_name)
        self.assertTrue(obj)

    def test_delete_object(self):
        self.storage.object_delete(self.account, self.container_name,
                                   self.object_name)
        self.assertRaises(exceptions.NoSuchObject, self.storage.object_fetch,
                          self.account, self.container_name, self.object_name)

    def test_list_account(self):
        containers, meta = self.storage.container_list(self.account)
        self.assertEqual(len(containers), 2)
        self.assertTrue(meta)
        self.assertEqual(meta['id'], self.account)
        self.assertEqual(meta['containers'], 2)
        self.assertTrue(meta['ctime'])
        self.assertEqual(meta['metadata'], {})

    def test_stat_account(self):
        info = self.storage.account_show(self.account)
        self.assertEqual(info['id'], self.account)
        self.assertEqual(info['containers'], 2)
        self.assertTrue(info['ctime'])
        self.assertEqual(info['metadata'], {})

    def _rain_test_download(self, data_size, broken_pos_list=[],
                            data_range=None):
        if len(self.conf['rawx']) < 10:
            self.skipTest("Not enough rawx. "
                          "Rain tests needs more than 10 rawx to run")

        test_data = os.urandom(data_size)
        object_name = "func-test-object-%s" % uuid.uuid4()
        self.storage.object_create(self.account, self.container_name,
                                   obj_name=object_name,
                                   policy="RAIN",
                                   data=test_data)

        meta, raw_stream = self.storage.object_analyze(
            self.account, self.container_name, object_name)

        for c in raw_stream:
            if c["pos"] in broken_pos_list:
                resp = self.session.delete(c["url"])
                resp.raise_for_status()

        if data_range is None:
            meta, stream = self.storage.object_fetch(
                self.account, self.container_name, object_name)

            test_data_hash = md5_data(test_data)
        else:
            begin, end = data_range
            meta, stream = self.storage.object_fetch(
                self.account, self.container_name, object_name,
                offset=begin, size=end-begin)

            test_data_hash = md5_data(test_data[begin:end])

        data = "".join(stream)
        data_hash = md5_data(data)
        self.assertEqual(data_hash, test_data_hash)

    def test_rain_fetch_object_0_b_without_broken_chunk(self):
        self._rain_test_download(0)

    def test_rain_fetch_object_0_b_with_broken_chunk_0_0(self):
        self._rain_test_download(0, broken_pos_list=["0.0"])

    def test_rain_fetch_object_1_b_without_broken_chunk(self):
        self._rain_test_download(1)

    def test_rain_fetch_object_1_b_with_broken_chunk_0_1_and_0_p0(self):
        self._rain_test_download(1, broken_pos_list=["0.0", "0.p0"])

    def test_rain_fetch_object_100_b_without_broken_chunk(self):
        self._rain_test_download(100)

    def test_rain_fetch_object_chunksize_b_without_broken_chunk(self):
        self._rain_test_download(self.chunk_size)

    def test_rain_fetch_object_chunksize_bytes_with_broken_chunk_0_5(self):
        self._rain_test_download(self.chunk_size, broken_pos_list=["0.5"])

    def test_rain_fetch_object_2xchunksize_b_without_broken_chunk(self):
        self._rain_test_download(2 * self.chunk_size)

    def test_rain_fetch_object_2xchksize_b_with_bc_0_1_a_1_5_a_1_p1(self):
        self._rain_test_download(2 * self.chunk_size,
                                 broken_pos_list=["0.1", "1.5", "1.p1"])

    def test_rain_fetch_object_100_bytes_range_0_10_without_broken_chunk(self):
        self._rain_test_download(100, data_range=(0, 10))

    def test_rain_fetch_object_chunksize_b_range_end_without_broken_chk(self):
        self._rain_test_download(
            self.chunk_size,
            data_range=(self.chunk_size - 30, self.chunk_size))

    def test_rain_fetch_object_2xcksize_b_range_overlap_without_brkn_chk(self):
        self._rain_test_download(
            2 * self.chunk_size,
            data_range=(self.chunk_size / 2, self.chunk_size + 1))

    def test_rain_fetch_object_100_bytes_range_0_10_with_broken_chunks(self):
        self._rain_test_download(100, data_range=(0, 10),
                                 broken_pos_list=["0.0", "0.1"])

    def test_rain_fetch_object_chunksize_b_range_end_with_broken_chunks(self):
        self._rain_test_download(
            self.chunk_size,
            data_range=(self.chunk_size - 30, self.chunk_size),
            broken_pos_list=["0.5", "0.p0"])

    def test_rain_fetch_object_2xcksize_b_range_overlap_with_broken_chks(self):
        self._rain_test_download(
            2 * self.chunk_size,
            data_range=(self.chunk_size / 2, self.chunk_size + 1),
            broken_pos_list=["0.4", "0.p1", "1.0"])

    def test_rain_fetch_object_not_enough_chunks(self):
        with ExpectedException(OioException):
            self._rain_test_download(self.chunk_size,
                                     broken_pos_list=["0.0", "0.p0", "0.p1"])
