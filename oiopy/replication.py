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
from oiopy.exceptions import ConnectionTimeout, ChunkReadTimeout, \
    ChunkWriteTimeout, ClientReadTimeout, SourceReadError
from oiopy import utils
from oiopy import io
from oiopy.http import http_connect
from oiopy.constants import chunk_headers


logger = logging.getLogger(__name__)


class ReplicatedChunkWriteHandler(object):
    def __init__(self, sysmeta, meta_chunk, checksum):
        self.sysmeta = sysmeta
        self.meta_chunk = meta_chunk
        self.checksum = checksum

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
                h[chunk_headers["content_size"]] = \
                    self.sysmeta['content_length']
                h[chunk_headers["content_chunkmethod"]] = \
                    self.sysmeta['chunk_method']
                h[chunk_headers["content_mimetype"]] = \
                    self.sysmeta['mime_type']
                h[chunk_headers["content_policy"]] = self.sysmeta['policy']
                h[chunk_headers["content_chunksnb"]] = \
                    self.sysmeta['content_chunksnb']
                h[chunk_headers["container_id"]] = self.sysmeta['container_id']
                h[chunk_headers["chunk_pos"]] = chunk["pos"]
                h[chunk_headers["chunk_id"]] = chunk_path
                with ConnectionTimeout(io.CONNECTION_TIMEOUT):
                    conn = http_connect(parsed.netloc, 'PUT', parsed.path, h)
                    conn.chunk = chunk
                return conn
            except (Exception, Timeout):
                pass

        meta_chunk = self.meta_chunk

        pile = GreenPile(len(meta_chunk))

        for chunk in meta_chunk:
            pile.spawn(_connect_put, chunk)

        conns = [conn for conn in pile if conn]

        min_conns = 1

        if len(conns) < min_conns:
            raise exc.OioException("RAWX connection failure")

        bytes_transferred = 0
        try:
            with utils.ContextPool(len(meta_chunk)) as pool:
                for conn in conns:
                    conn.failed = False
                    conn.queue = Queue(io.PUT_QUEUE_DEPTH)
                    pool.spawn(self._send_data, conn)

                while True:
                    remaining_bytes = size - bytes_transferred
                    if io.WRITE_CHUNK_SIZE < remaining_bytes:
                        read_size = io.WRITE_CHUNK_SIZE
                    else:
                        read_size = remaining_bytes
                    with ClientReadTimeout(io.CLIENT_TIMEOUT):
                        try:
                            data = source.read(read_size)
                        except (ValueError, IOError) as e:
                            raise SourceReadError(str(e))
                        if len(data) == 0:
                            for conn in conns:
                                conn.queue.put('0\r\n\r\n')
                            break
                    self.checksum.update(data)
                    bytes_transferred += len(data)
                    for conn in conns:
                        if not conn.failed:
                            conn.queue.put('%x\r\n%s\r\n' % (len(data),
                                                             data))
                        else:
                            conns.remove(conn)

                    if len(conns) < min_conns:
                        raise exc.OioException("RAWX write failure")

                for conn in conns:
                    if conn.queue.unfinished_tasks:
                        conn.queue.join()

            conns = [conn for conn in conns if not conn.failed]

        except SourceReadError:
            raise
        except ClientReadTimeout:
            raise
        except Timeout as e:
            raise exc.OioTimeout(str(e))
        except Exception as e:
            raise exc.OioException(
                "Exception during chunk write %s" % str(e))

        final_chunks = []
        for conn in conns:
            if conn.failed:
                continue
            pile.spawn(self._get_response, conn)

        def _handle_resp(conn, resp):
            resp.read()
            if resp.status >= 500:
                conn.failed = True
                logger.error("Wrong status code from %s (%s)",
                             conn.chunk, resp.status)
            elif resp.status == 201:
                conn.chunk["size"] = bytes_transferred
                final_chunks.append(conn.chunk)
            conn.close()

        for (conn, resp) in pile:
            if resp:
                _handle_resp(conn, resp)
            else:
                conn.failed = True
        if len(final_chunks) < min_conns:
            raise exc.OioException("RAWX write failure")

        meta_checksum = self.checksum.hexdigest()
        for chunk in final_chunks:
            chunk["hash"] = meta_checksum

        return bytes_transferred, meta_checksum, final_chunks

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
            handler = ReplicatedChunkWriteHandler(self.sysmeta, meta_chunk,
                                                  global_checksum)
            bytes_transferred, checksum, chunks = handler.stream(self.source,
                                                                 size)
            content_chunks += chunks
            total_bytes_transferred += bytes_transferred

        content_checksum = global_checksum.hexdigest()

        return content_chunks, total_bytes_transferred, content_checksum


class ReplicatedChunkDownloadHandler(object):
    def __init__(self, chunks, size, offset, headers=None):
        self.chunks = chunks
        self.failed_chunks = []

        headers = headers or {}
        h_range = "bytes=%d-" % offset
        end = None
        if size >= 0:
            end = (size + offset - 1)
            h_range += str(end)
        headers["Range"] = h_range
        self.headers = headers
        self.begin = offset
        self.end = end

    def get_stream(self):
        source = self._get_chunk_source()
        stream = None
        if source:
            stream = self._make_stream(source)
        return stream

    def _fast_forward(self, nb_bytes):
        self.begin += nb_bytes
        if self.end and self.begin > self.end:
            raise Exception('Requested Range Not Satisfiable')
        h_range = 'bytes=%d-' % self.begin
        if self.end:
            h_range += str(self.end)
        self.headers['Range'] = h_range

    def _get_chunk_source(self):
        source = None
        for chunk in self.chunks:
            try:
                with ConnectionTimeout(io.CONNECTION_TIMEOUT):
                    raw_url = chunk["url"]
                    parsed = urlparse(raw_url)
                    conn = http_connect(parsed.netloc, 'GET', parsed.path,
                                        self.headers)
                source = conn.getresponse(True)
                source.conn = conn

            except (Timeout, Exception):
                self.failed_chunks.append(chunk)
                continue
            if source.status not in (200, 206):
                self.failed_chunks.append(chunk)
                io.close_source(source)
                source = None
            else:
                break

        return source

    def _make_stream(self, source):
        bytes_read = 0
        try:
            while True:
                try:
                    with ChunkReadTimeout(io.CHUNK_TIMEOUT):
                        data = source.read(io.READ_CHUNK_SIZE)
                        bytes_read += len(data)
                except ChunkReadTimeout:
                    self._fast_forward(bytes_read)
                    new_source = self._get_chunk_source()
                    if new_source:
                        io.close_source(source)
                        source = new_source
                        bytes_read = 0
                        continue
                    else:
                        raise
                if not data:
                    break
                yield data
        except ChunkReadTimeout:
            # error while reading chunk
            raise
        except GeneratorExit:
            # client premature stop
            pass
        except Exception:
            # error
            raise
        finally:
            io.close_source(source)
