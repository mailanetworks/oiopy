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


import hashlib
from cStringIO import StringIO
from urlparse import urlparse
import json
from functools import wraps

import os
from eventlet import Timeout
from eventlet.greenpool import GreenPile
from eventlet.queue import Queue

from oiopy.api import API
from oiopy.directory import DirectoryAPI
from oiopy import exceptions
from oiopy import utils
from oiopy.exceptions import ConnectionTimeout, ChunkReadTimeout, \
    ChunkWriteTimeout
from oiopy.http import http_connect


CONTAINER_METADATA_PREFIX = "x-oio-container-meta-"
OBJECT_METADATA_PREFIX = "x-oio-content-meta-"

WRITE_CHUNK_SIZE = 65536
READ_CHUNK_SIZE = 65536

CONNECTION_TIMEOUT = 2
CHUNK_TIMEOUT = 3

PUT_QUEUE_DEPTH = 10

container_headers = {
    "size": "%ssys-m2-usage" % CONTAINER_METADATA_PREFIX,
    "ns": "%ssys-ns" % CONTAINER_METADATA_PREFIX
}

object_headers = {
    "name": "%sname" % OBJECT_METADATA_PREFIX,
    "policy": "%spolicy" % OBJECT_METADATA_PREFIX,
    "version": "%sversion" % OBJECT_METADATA_PREFIX,
    "content_type": "%smime-type" % OBJECT_METADATA_PREFIX,
    "size": "%slength" % OBJECT_METADATA_PREFIX,
    "ctime": "%sctime" % OBJECT_METADATA_PREFIX,
    "hash": "%shash" % OBJECT_METADATA_PREFIX,
    "chunk_method": "%schunk-method" % OBJECT_METADATA_PREFIX
}


def handle_container_not_found(fnc):
    @wraps(fnc)
    def _wrapped(self, account, container, *args, **kwargs):
        try:
            return fnc(self, account, container, *args, **kwargs)
        except exceptions.NotFound as e:
            e.message = "Container '%s' does not exist." % container
            raise exceptions.NoSuchContainer(e)

    return _wrapped


def handle_object_not_found(fnc):
    @wraps(fnc)
    def _wrapped(self, account, container, obj, *args, **kwargs):
        try:
            return fnc(self, account, container, obj, *args, **kwargs)
        except exceptions.NotFound as e:
            e.message = "Object '%s' does not exist." % obj
            raise exceptions.NoSuchObject(e)

    return _wrapped


def _sort_chunks(raw_chunks, rain_security):
    chunks = dict()
    for chunk in raw_chunks:
        raw_position = chunk["pos"].split(".")
        position = int(raw_position[0])
        if rain_security:
            subposition = raw_position[1]
        if position in chunks:
            if rain_security:
                chunks[position][subposition] = chunk
            else:
                chunks[position].append(chunk)
        else:
            if rain_security:
                chunks[position] = dict()
                chunks[position][subposition] = chunk
            else:
                chunks[position] = [chunk]
    return chunks


def _make_object_metadata(headers):
    meta = {}
    prefix = OBJECT_METADATA_PREFIX

    for k, v in headers.iteritems():
        k = k.lower()
        if k.startswith(prefix):
            key = k.replace(prefix, "")
            meta[key] = v
    return meta


class ObjectStorageAPI(API):
    """
    The Object Storage API
    """

    def __init__(self, namespace, endpoint, **kwargs):
        endpoint_v2 = '/'.join([endpoint.rstrip('/'), 'v2.0'])
        super(ObjectStorageAPI, self).__init__(endpoint=endpoint_v2, **kwargs)
        self.directory = DirectoryAPI(namespace, endpoint, session=self.session)
        self.namespace = namespace

    def account_create(self, account, headers=None):
        uri = '/v1.0/account/create'
        account_id = utils.quote(account, '')
        params = {'id': account_id}
        resp, resp_body = self._account_request('PUT', uri, params=params,
                                                headers=headers)
        created = (resp.status_code == 201)
        return created

    def account_show(self, account, headers=None):
        uri = "/v1.0/account/show"
        account_id = utils.quote(account, '')
        params = {'id': account_id}
        resp, resp_body = self._account_request('GET', uri, params=params,
                                                headers=headers)
        return resp_body

    def account_update(self, account, metadata, to_delete=None, headers=None):
        uri = "/v1.0/account/update"
        account_id = utils.quote(account, '')
        params = {'id': account_id}
        data = json.dumps({"metadata": metadata, "to_delete": to_delete})
        resp, resp_body = self._account_request('POST', uri, params=params,
                                                data=data, headers=headers)

    def container_create(self, account, container, headers=None):
        try:
            self.directory.link(account, container, "meta2", headers=headers)
        except exceptions.NotFound:
            self.directory.create(account, container, headers=headers)
            self.directory.link(account, container, "meta2", headers=headers)

        uri = self._make_uri(account, container)

        resp, resp_body = self._request('PUT', uri, headers=headers)
        if resp.status_code not in (204, 201):
            raise exceptions.from_response(resp, resp_body)

    @handle_container_not_found
    def container_delete(self, account, container, headers=None):
        uri = self._make_uri(account, container)
        try:
            resp, resp_body = self._request('DELETE', uri, headers=headers)
        except exceptions.Conflict as e:
            raise exceptions.ContainerNotEmpty(e)

        self.directory.unlink(account, container, "meta2", headers=headers)

    def container_list(self, account, limit=None, marker=None,
                       end_marker=None, prefix=None, delimiter=None,
                       headers=None):
        uri = "v1.0/account/containers"
        account_id = utils.quote(account, '')
        params = {"id": account_id, "limit": limit, "marker": marker,
                  "delimiter": delimiter, "prefix": prefix,
                  "end_marker": end_marker}

        resp, resp_body = self._account_request('GET', uri, params=params,
                                                headers=headers)
        listing = resp_body['listing']
        del resp_body['listing']
        return listing, resp_body

    @handle_container_not_found
    def container_show(self, account, container, headers=None):
        uri = self._make_uri(account, container)
        resp, resp_body = self._action(uri, 'GetProperties', None,
                                       headers=headers)
        return resp_body

    @handle_container_not_found
    def container_update(self, account, container, metadata, clear=False,
                         headers=None):
        uri = self._make_uri(account, container)

        if not metadata:
            resp, resp_body = self._action(uri, 'DelProperties', [],
                                           headers=headers)
        else:
            params = {'flush': 1} if clear else {}
            resp, resp_body = self._action(uri, 'SetProperties', metadata,
                                           headers=headers, params=params)

    @handle_container_not_found
    def object_create(self, account, container, file_or_path=None, data=None,
                      etag=None, obj_name=None, content_type=None,
                      content_encoding=None, content_length=None, metadata=None,
                      headers=None):
        if (data, file_or_path) == (None, None):
            raise exceptions.MissingData()
        src = data if data is not None else file_or_path
        if src is file_or_path:
            if isinstance(file_or_path, basestring):
                if not os.path.exists(file_or_path):
                    raise exceptions.FileNotFound("File '%s' not found." %
                                                  file_or_path)
                file_name = os.path.basename(file_or_path)
            else:
                try:
                    file_name = os.path.basename(file_or_path.name)
                except AttributeError:
                    file_name = None
            obj_name = obj_name or file_name
        if not obj_name:
            raise exceptions.MissingName("No name for the object has been "
                                         "specified")

        if isinstance(data, basestring):
            content_length = len(data)

        if content_length is None:
            raise exceptions.MissingContentLength()

        sysmeta = {'content_type': content_type,
                   'content_encoding': content_encoding,
                   'content_length': content_length,
                   'etag': etag}

        if src is data:
            self._object_create(account, container, obj_name, StringIO(data),
                                sysmeta, headers=headers)
        elif hasattr(file_or_path, "read"):
            self._object_create(account, container, obj_name, src, sysmeta,
                                headers=headers)
        else:
            with open(file_or_path, "rb") as f:
                self._object_create(account, container, obj_name, f, sysmeta,
                                    headers=headers)

    @handle_object_not_found
    def object_delete(self, account, container, obj, headers=None):
        uri = self._make_uri(account, container, obj)
        resp, resp_body = self._request('DELETE', uri, headers=headers)

    @handle_container_not_found
    def object_list(self, account, container, limit=None, marker=None,
                    delimiter=None, prefix=None, end_marker=None,
                    headers=None):
        uri = self._make_uri(account, container)
        d = {"max": limit, "marker": marker, "delimiter": delimiter,
             "prefix": prefix, "end_marker": end_marker}
        query_string = "&".join(["%s=%s" % (k, v) for k, v in d.iteritems()
                                 if v is not None])

        if query_string:
            uri = "%s?%s" % (uri, query_string)
        resp, resp_body = self._request('GET', uri, headers=headers)

        return resp_body

    @handle_object_not_found
    def object_fetch(self, account, container, obj, size=None, offset=0,
                     headers=None):
        uri = self._make_uri(account, container, obj)
        resp, resp_body = self._request('GET', uri, headers=headers)

        meta = _make_object_metadata(resp.headers)
        raw_chunks = resp_body

        rain_security = len(raw_chunks[0]["pos"].split(".")) == 2
        chunks = _sort_chunks(raw_chunks, rain_security)
        stream = self._fetch_stream(meta, chunks, rain_security, size, offset,
                                    headers=headers)
        return meta, stream

    @handle_object_not_found
    def object_show(self, account, container, obj, headers=None):
        uri = self._make_uri(account, container, obj)
        resp, resp_body = self._action(uri, 'GetProperties', None,
                                       headers=headers)

        meta = _make_object_metadata(resp.headers)
        for k, v in resp_body.iteritems():
            meta[k] = v
        return meta

    @handle_object_not_found
    def object_update(self, account, container, obj, metadata, clear=False,
                      headers=None):
        uri = self._make_uri(account, container, obj)
        if clear:
            resp, resp_body = self._action(uri, 'DelProperties', [],
                                           headers=headers)
        if metadata:
            args = metadata
            resp, resp_body = self._action(uri, 'SetProperties', args,
                                           headers=headers)

    def _make_uri(self, account, container, obj=None):
        account = utils.quote(account, '')
        container = utils.quote(container, '')
        uri = "m2/%s/%s/%s" % (self.namespace, account, container)
        if obj:
            obj = utils.quote(obj, '')
            uri += '/%s' % obj
        return uri

    def _action(self, uri, action, args, headers=None, **kwargs):
        uri = "%s/action" % uri
        body = {"action": action, "args": args}
        data = json.dumps(body)
        return self._request('POST', uri, data=data, headers=headers, **kwargs)

    def _get_account_url(self):
        uri = 'lb/%s/account' % self.namespace
        resp, resp_body = self._request('GET', uri)
        if resp.status_code == 200:
            instance_info = resp_body[0]
            return 'http://%s/' % instance_info['addr']
        else:
            raise exceptions.ClientException("could not find account instance "
                                             "url")

    def _account_request(self, method, uri, **kwargs):
        account_url = self._get_account_url()
        resp, resp_body = self._request(method, uri, endpoint=account_url,
                                        **kwargs)
        return resp, resp_body

    def _object_create(self, account, container, obj_name, src,
                       sysmeta, headers=None):
        uri = self._make_uri(account, container, obj_name)
        args = {'size': sysmeta['content_length']}
        resp, resp_body = self._action(uri, 'Beans', args, headers=headers)

        raw_chunks = resp_body

        rain_security = len(raw_chunks[0]["pos"].split(".")) == 2
        if rain_security:
            raise exceptions.OioException('RAIN Security not supported.')

        chunks = _sort_chunks(raw_chunks, rain_security)
        final_chunks, bytes_transferred, content_checksum = self._put_stream(
            obj_name, src, sysmeta, chunks, headers=headers)

        sysmeta['etag'] = content_checksum

        headers = {"x-oio-content-meta-length": bytes_transferred,
                   "x-oio-content-meta-hash": sysmeta['etag'],
                   "content-type": sysmeta['content_type']}

        data = json.dumps(final_chunks)
        resp, resp_body = self._request('PUT', uri, data=data, headers=headers)

    def _put_stream(self, obj_name, src, sysmeta, chunks, headers=None):
        global_checksum = hashlib.md5()
        total_bytes_transferred = 0
        content_chunks = []

        def _connect_put(chunk):
            raw_url = chunk["url"]
            parsed = urlparse(raw_url)
            try:
                chunk_path = parsed.path.split('/')[-1]
                headers = {}
                headers["transfer-encoding"] = "chunked"
                headers["content_path"] = utils.quote(obj_name)
                headers["content_size"] = sysmeta['content_length']
                headers["content_chunksnb"] = len(chunks)
                headers["chunk_position"] = chunk["pos"]
                headers["chunk_id"] = chunk_path

                with ConnectionTimeout(CONNECTION_TIMEOUT):
                    conn = http_connect(parsed.netloc, 'PUT', parsed.path,
                                        headers)
                    conn.chunk = chunk
                return conn
            except (Exception, Timeout) as e:
                pass

        def _send_data(conn):
            while True:
                data = conn.queue.get()
                if not conn.failed:
                    try:
                        with ChunkWriteTimeout(CHUNK_TIMEOUT):
                            conn.send(data)
                    except (Exception, ChunkWriteTimeout):
                        conn.failed = True
                conn.queue.task_done()

        for pos in range(len(chunks)):
            current_chunks = chunks[pos]

            pile = GreenPile(len(current_chunks))

            for current_chunk in current_chunks:
                pile.spawn(_connect_put, current_chunk)

            conns = [conn for conn in pile if conn]

            min_conns = 1

            if len(conns) < min_conns:
                raise exceptions.OioException("RAWX connection failure")

            bytes_transferred = 0
            total_size = current_chunks[0]["size"]
            chunk_checksum = hashlib.md5()
            try:
                with utils.ContextPool(len(current_chunks)) as pool:
                    for conn in conns:
                        conn.failed = False
                        conn.queue = Queue(PUT_QUEUE_DEPTH)
                        pool.spawn(_send_data, conn)

                    while True:
                        remaining_bytes = total_size - bytes_transferred
                        if WRITE_CHUNK_SIZE < remaining_bytes:
                            read_size = WRITE_CHUNK_SIZE
                        else:
                            read_size = remaining_bytes
                        with ChunkReadTimeout(CHUNK_TIMEOUT):
                            data = src.read(read_size)
                            if len(data) == 0:
                                for conn in conns:
                                    conn.queue.put('0\r\n\r\n')
                                break
                        chunk_checksum.update(data)
                        global_checksum.update(data)
                        bytes_transferred += len(data)
                        for conn in conns:
                            if not conn.failed:
                                conn.queue.put('%x\r\n%s\r\n' % (len(data),
                                                                 data))
                            else:
                                conns.remove(conn)

                        if len(conns) < min_conns:
                            raise exceptions.OioException("RAWX write failure")

                    for conn in conns:
                        if conn.queue.unfinished_tasks:
                            conn.queue.join()

                conns = [conn for conn in conns if not conn.failed]

            except ChunkReadTimeout:
                raise exceptions.ClientReadTimeout()
            except (Exception, Timeout):
                raise exceptions.OioException("Exception during chunk "
                                              "write.")

            final_chunks = []
            for conn in conns:
                resp = conn.getresponse()
                if resp.status in (200, 201):
                    conn.chunk["size"] = bytes_transferred
                    final_chunks.append(conn.chunk)
                conn.close()
            if len(final_chunks) < min_conns:
                raise exceptions.OioException("RAWX write failure")

            checksum = chunk_checksum.hexdigest()
            for chunk in final_chunks:
                chunk["hash"] = checksum
            content_chunks += final_chunks
            total_bytes_transferred += bytes_transferred

        content_checksum = global_checksum.hexdigest()

        return content_chunks, total_bytes_transferred, content_checksum

    def _fetch_stream(self, meta, chunks, rain_security, size, offset,
                      headers=None):
        current_offset = 0
        total_bytes = 0
        if size is None:
            size = int(meta["length"])
        if rain_security:
            raise exceptions.OioException("RAIN not supported")
        else:
            for pos in range(len(chunks)):
                chunk_size = int(chunks[pos][0]["size"])
                if total_bytes >= size:
                    break
                if current_offset + chunk_size > offset:
                    if current_offset < offset:
                        _offset = offset - current_offset
                    else:
                        _offset = 0
                    if chunk_size + total_bytes > size:
                        _size = size - total_bytes
                    else:
                        _size = chunk_size

                    handler = ChunkDownloadHandler(chunks[pos], _size, _offset)
                    stream = handler.get_stream()
                    if not stream:
                        raise exceptions.OioException("Error while downloading")
                    for s in stream:
                        total_bytes += len(s)
                        yield s
                current_offset += chunk_size


class ChunkDownloadHandler(object):
    def __init__(self, chunks, size, offset, headers=None):
        self.chunks = chunks
        self.failed_chunks = []

        headers = {}
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
                with ConnectionTimeout(CONNECTION_TIMEOUT):
                    raw_url = chunk["url"]
                    parsed = urlparse(raw_url)
                    conn = http_connect(parsed.netloc, 'GET', parsed.path,
                                        self.headers)
                source = conn.getresponse(True)
                source.conn = conn

            except Exception as e:
                self.failed_chunks.append(chunk)
                continue
            if source.status not in (200, 206):
                self.failed_chunks.append(chunk)
                source.conn.close()
                source = None
            else:
                break

        return source

    def _make_stream(self, source):
        bytes_read = 0
        try:
            while True:
                try:
                    data = source.read(READ_CHUNK_SIZE)
                    bytes_read += len(data)
                except ChunkReadTimeout:
                    self._fast_forward(bytes_read)
                    new_source = self._get_chunk_source()
                    if new_source:
                        source.conn.close()
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
            source.conn.close()
