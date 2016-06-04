# Copyright (C) 2016 OpenIO SAS

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

import logging
import hashlib
from eventlet import Timeout, GreenPile
from eventlet.queue import Queue
from urlparse import urlparse
from oiopy import exceptions as exc
from oiopy.exceptions import ConnectionTimeout, \
    ChunkWriteTimeout, SourceReadError, SourceReadTimeout
from oiopy import utils
from oiopy import io
from oiopy.constants import chunk_headers


logger = logging.getLogger(__name__)


class ReplicatedChunkWriteHandler(object):
    def __init__(self, sysmeta, meta_chunk, checksum, storage_method):
        self.sysmeta = sysmeta
        self.meta_chunk = meta_chunk
        self.checksum = checksum
        self.storage_method = storage_method

    def _check_quorum(self, conns):
        return len(conns) >= self.storage_method.quorum

    def stream(self, source, size):
        bytes_transferred = 0

        def _connect_put(chunk):
            raw_url = chunk["url"]
            parsed = urlparse(raw_url)
            try:
                chunk_path = parsed.path.split('/')[-1]
                h = {}
                h["transfer-encoding"] = "chunked"
                h[chunk_headers["content_id"]] = self.sysmeta['id']
                h[chunk_headers["content_version"]] = self.sysmeta['version']
                h[chunk_headers["content_path"]] = \
                    utils.quote(self.sysmeta['content_path'])
                h[chunk_headers["content_chunkmethod"]] = \
                    self.sysmeta['chunk_method']
                h[chunk_headers["content_policy"]] = self.sysmeta['policy']
                h[chunk_headers["container_id"]] = self.sysmeta['container_id']
                h[chunk_headers["chunk_pos"]] = chunk["pos"]
                h[chunk_headers["chunk_id"]] = chunk_path
                with ConnectionTimeout(io.CONNECTION_TIMEOUT):
                    conn = io.http_connect(
                        parsed.netloc, 'PUT', parsed.path, h)
                    conn.chunk = chunk
                return conn, chunk
            except (Exception, Timeout) as e:
                msg = str(e)
                logger.error("Failed to connect to %s (%s)", chunk, msg)
                chunk['error'] = msg
                return None, chunk

        meta_chunk = self.meta_chunk

        pile = GreenPile(len(meta_chunk))

        failed_chunks = []

        current_conns = []

        for chunk in meta_chunk:
            pile.spawn(_connect_put, chunk)

        results = [d for d in pile]

        for conn, chunk in results:
            if not conn:
                failed_chunks.append(chunk)
            else:
                current_conns.append(conn)

        quorum = False
        if current_conns:
            quorum = self._check_quorum(current_conns)
        if not quorum:
            raise exc.OioException("RAWX write failure")

        bytes_transferred = 0
        try:
            with utils.ContextPool(len(meta_chunk)) as pool:
                for conn in current_conns:
                    conn.failed = False
                    conn.queue = Queue(io.PUT_QUEUE_DEPTH)
                    pool.spawn(self._send_data, conn)

                while True:
                    remaining_bytes = size - bytes_transferred
                    if io.WRITE_CHUNK_SIZE < remaining_bytes:
                        read_size = io.WRITE_CHUNK_SIZE
                    else:
                        read_size = remaining_bytes
                    with SourceReadTimeout(io.CLIENT_TIMEOUT):
                        try:
                            data = source.read(read_size)
                        except (ValueError, IOError) as e:
                            raise SourceReadError(str(e))
                        if len(data) == 0:
                            for conn in current_conns:
                                conn.queue.put('0\r\n\r\n')
                            break
                    self.checksum.update(data)
                    bytes_transferred += len(data)
                    for conn in current_conns:
                        if not conn.failed:
                            conn.queue.put('%x\r\n%s\r\n' % (len(data),
                                                             data))
                        else:
                            current_conns.remove(conn)

                    quorum = self._check_quorum(current_conns)
                    if not quorum:
                        raise exc.OioException("RAWX write failure")

                for conn in current_conns:
                    if conn.queue.unfinished_tasks:
                        conn.queue.join()

        except SourceReadTimeout:
            logger.warn('Source read timeout')
            raise
        except SourceReadError:
            logger.warn('Source read error')
            raise
        except Timeout:
            logger.exception('Timeout writing data')
            raise
        except Exception:
            logger.exception('Exception writing data')
            raise

        success_chunks = []

        for conn in current_conns:
            if conn.failed:
                failed_chunks.append(conn.chunk)
                continue
            pile.spawn(self._get_response, conn)

        def _handle_resp(conn, resp):
            if resp:
                if resp.status == 201:
                    success_chunks.append(conn.chunk)
                else:
                    conn.failed = True
                    conn.chunk['error'] = 'HTTP %s' % resp.status
                    failed_chunks.append(conn.chunk)
                    logger.error("Wrong status code from %s (%s)",
                                 conn.chunk, resp.status)
            conn.close()

        for (conn, resp) in pile:
            if resp:
                _handle_resp(conn, resp)
        quorum = self._check_quorum(success_chunks)
        if not quorum:
            raise exc.OioException("RAWX write failure")

        meta_checksum = self.checksum.hexdigest()
        for chunk in success_chunks:
            chunk["size"] = bytes_transferred
            chunk["hash"] = meta_checksum

        return bytes_transferred, meta_checksum, success_chunks + failed_chunks

    def _send_data(self, conn):
        while True:
            data = conn.queue.get()
            if not conn.failed:
                try:
                    with ChunkWriteTimeout(io.CHUNK_TIMEOUT):
                        conn.send(data)
                except (Exception, ChunkWriteTimeout):
                    conn.failed = True
            conn.queue.task_done()

    def _get_response(self, conn):
        try:
            resp = conn.getresponse(True)
        except (Exception, Timeout):
            resp = None
            logger.exception("Failed to read response %s", conn.chunk)
        return (conn, resp)


class ReplicatedWriteHandler(io.WriteHandler):
    def stream(self):
        global_checksum = hashlib.md5()
        total_bytes_transferred = 0
        content_chunks = []

        for pos in range(len(self.chunks)):
            meta_chunk = self.chunks[pos]

            # chunks are all identical
            # so take the first size
            size = meta_chunk[0]["size"]
            handler = ReplicatedChunkWriteHandler(
                self.sysmeta, meta_chunk, global_checksum, self.storage_method)
            bytes_transferred, checksum, chunks = handler.stream(self.source,
                                                                 size)
            content_chunks += chunks
            total_bytes_transferred += bytes_transferred

        content_checksum = global_checksum.hexdigest()

        return content_chunks, total_bytes_transferred, content_checksum
