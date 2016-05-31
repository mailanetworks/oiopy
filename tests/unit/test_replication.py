import unittest
from collections import defaultdict
from cStringIO import StringIO
from hashlib import md5
from eventlet import Timeout
from oiopy import exceptions as exc
from oiopy.fakes import set_http_connect
from oiopy.replication import ReplicatedChunkWriteHandler
from oiopy.storage_method import STORAGE_METHODS
from tests.unit import CHUNK_SIZE, EMPTY_CHECKSUM, empty_stream, \
    decode_chunked_body


class TestReplication(unittest.TestCase):
    def setUp(self):
        self.chunk_method = 'plain/nb_copy=3'
        storage_method = STORAGE_METHODS.load(self.chunk_method)
        self.storage_method = storage_method
        self.cid = \
            '3E32B63E6039FD3104F63BFAE034FADAA823371DD64599A8779BA02B3439A268'
        self.sysmeta = {
            'id': '705229BB7F330500A65C3A49A3116B83',
            'version': '1463998577463950',
            'chunk_method': self.chunk_method,
            'container_id': self.cid,
            'policy': 'REPLI3',
            'content_path': 'test',
        }

        self._meta_chunk = [
                {'url': 'http://127.0.0.1:7000/0', 'pos': '0'},
                {'url': 'http://127.0.0.1:7001/1', 'pos': '0'},
                {'url': 'http://127.0.0.1:7002/2', 'pos': '0'},
        ]

    def meta_chunk(self):
        return self._meta_chunk

    def checksum(self, d=''):
        return md5(d)

    def test_write_simple(self):
        checksum = self.checksum()
        source = empty_stream()
        meta_chunk = self.meta_chunk()
        size = CHUNK_SIZE
        resps = [201] * len(meta_chunk)
        with set_http_connect(*resps):
            handler = ReplicatedChunkWriteHandler(
                self.sysmeta, meta_chunk, checksum, self.storage_method)
            bytes_transferred, checksum, chunks = handler.stream(
                source, size)
            self.assertEqual(len(chunks), len(meta_chunk))
            self.assertEqual(bytes_transferred, 0)
            self.assertEqual(checksum, EMPTY_CHECKSUM)

    def test_write_exception(self):
        checksum = self.checksum()
        source = empty_stream()
        meta_chunk = self.meta_chunk()
        size = CHUNK_SIZE
        resps = [500] * len(meta_chunk)
        with set_http_connect(*resps):
            handler = ReplicatedChunkWriteHandler(
                self.sysmeta, meta_chunk, checksum, self.storage_method)
            self.assertRaises(exc.OioException, handler.stream, source, size)

    def test_write_quorum_success(self):
        checksum = self.checksum()
        source = empty_stream()
        size = CHUNK_SIZE
        meta_chunk = self.meta_chunk()
        quorum_size = self.storage_method.quorum
        resps = [201] * quorum_size
        resps += [500] * (len(meta_chunk) - quorum_size)
        with set_http_connect(*resps):
            handler = ReplicatedChunkWriteHandler(
                self.sysmeta, meta_chunk, checksum, self.storage_method)
            bytes_transferred, checksum, chunks = handler.stream(source, size)

            self.assertEqual(len(chunks), len(meta_chunk))

            for i in range(quorum_size):
                self.assertEqual(chunks[i].get('error'), None)

            for i in xrange(quorum_size, len(meta_chunk)):
                self.assertEqual(chunks[i].get('error'), 'HTTP 500')

            self.assertEqual(bytes_transferred, 0)
            self.assertEqual(checksum, EMPTY_CHECKSUM)

    def test_write_quorum_error(self):
        checksum = self.checksum()
        source = empty_stream()
        size = CHUNK_SIZE
        meta_chunk = self.meta_chunk()
        quorum_size = self.storage_method.quorum
        resps = [500] * quorum_size
        resps += [201] * (len(meta_chunk) - quorum_size)
        with set_http_connect(*resps):
            handler = ReplicatedChunkWriteHandler(
                self.sysmeta, meta_chunk, checksum, self.storage_method)
            self.assertRaises(exc.OioException, handler.stream, source, size)

    def test_write_timeout(self):
        checksum = self.checksum()
        source = empty_stream()
        size = CHUNK_SIZE
        meta_chunk = self.meta_chunk()
        resps = [201] * (len(meta_chunk) - 1)
        resps.append(Timeout(1.0))
        with set_http_connect(*resps):
            handler = ReplicatedChunkWriteHandler(
                self.sysmeta, meta_chunk, checksum, self.storage_method)
            bytes_transferred, checksum, chunks = handler.stream(source, size)

        self.assertEqual(len(chunks), len(meta_chunk))

        for i in range(len(meta_chunk) - 1):
            self.assertEqual(chunks[i].get('error'), None)
        self.assertEqual(
            chunks[len(meta_chunk) - 1].get('error'), '1.0 second')

        self.assertEqual(bytes_transferred, 0)
        self.assertEqual(checksum, EMPTY_CHECKSUM)

    def test_write_partial_exception(self):
        checksum = self.checksum()
        source = empty_stream()
        size = CHUNK_SIZE
        meta_chunk = self.meta_chunk()

        resps = [201] * (len(meta_chunk) - 1)
        resps.append(Exception("failure"))
        with set_http_connect(*resps):
            handler = ReplicatedChunkWriteHandler(
                self.sysmeta, meta_chunk, checksum, self.storage_method)
            bytes_transferred, checksum, chunks = handler.stream(source, size)
        self.assertEqual(len(chunks), len(meta_chunk))
        for i in range(len(meta_chunk) - 1):
            self.assertEqual(chunks[i].get('error'), None)
        self.assertEqual(chunks[len(meta_chunk) - 1].get('error'), 'failure')

        self.assertEqual(bytes_transferred, 0)
        self.assertEqual(checksum, EMPTY_CHECKSUM)

    def test_write_error_source(self):
        class TestReader(object):
            def read(self, size):
                raise IOError('failure')

        checksum = self.checksum()
        source = TestReader()
        size = CHUNK_SIZE
        meta_chunk = self.meta_chunk()
        nb = len(meta_chunk)
        resps = [201] * nb
        with set_http_connect(*resps):
            handler = ReplicatedChunkWriteHandler(
                self.sysmeta, meta_chunk, checksum, self.storage_method)
            self.assertRaises(exc.SourceReadError, handler.stream, source,
                              size)

    def test_write_timeout_source(self):
        class TestReader(object):
            def read(self, size):
                raise Timeout(1.0)

        checksum = self.checksum()
        source = TestReader()
        size = CHUNK_SIZE
        meta_chunk = self.meta_chunk()
        nb = len(meta_chunk)
        resps = [201] * nb
        with set_http_connect(*resps):
            handler = ReplicatedChunkWriteHandler(
                self.sysmeta, meta_chunk, checksum, self.storage_method)
            self.assertRaises(Timeout, handler.stream, source,
                              size)

    def test_write_exception_source(self):
        class TestReader(object):
            def read(self, size):
                raise Exception('failure')

        checksum = self.checksum()
        source = TestReader()
        size = CHUNK_SIZE
        meta_chunk = self.meta_chunk()
        nb = len(meta_chunk)
        resps = [201] * nb
        with set_http_connect(*resps):
            handler = ReplicatedChunkWriteHandler(
                self.sysmeta, meta_chunk, checksum, self.storage_method)
            # TODO specialize exception
            self.assertRaises(Exception, handler.stream, source,
                              size)

    def test_write_transfer(self):
        checksum = self.checksum()
        test_data = ('1234' * 1024)[:-10]
        size = len(test_data)
        meta_chunk = self.meta_chunk()
        nb = len(meta_chunk)
        resps = [201] * nb
        source = StringIO(test_data)

        put_reqs = defaultdict(lambda: {'parts': []})

        def cb_body(conn_id, part):
            put_reqs[conn_id]['parts'].append(part)

        with set_http_connect(*resps, cb_body=cb_body):
            handler = ReplicatedChunkWriteHandler(
                self.sysmeta, meta_chunk, checksum, self.storage_method)
            bytes_transferred, checksum, chunks = handler.stream(source, size)

        final_checksum = self.checksum(test_data).hexdigest()
        self.assertEqual(len(test_data), bytes_transferred)
        self.assertEqual(final_checksum, checksum)

        bodies = []

        for conn_id, info in put_reqs.items():
            body, trailers = decode_chunked_body(''.join(info['parts']))
            # TODO check trailers?
            bodies.append(body)

        self.assertEqual(len(bodies), nb)
        for body in bodies:
            self.assertEqual(len(test_data), len(body))
            self.assertEqual(self.checksum(body).hexdigest(), final_checksum)
