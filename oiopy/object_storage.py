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


from cStringIO import StringIO
from functools import wraps
import hashlib
import json
import os
from urlparse import urlparse

from eventlet import Timeout
from eventlet.greenpool import GreenPile
from eventlet.queue import Queue

from oiopy.api import API
from oiopy.directory import DirectoryAPI
from oiopy import exceptions as exc
from oiopy import utils
from oiopy.exceptions import ConnectionTimeout, ChunkReadTimeout, \
    ChunkWriteTimeout
from oiopy.http import http_connect


CONTAINER_METADATA_PREFIX = "x-oio-container-meta-"
OBJECT_METADATA_PREFIX = "x-oio-content-meta-"
CHUNK_METADATA_PREFIX = "x-oio-chunk-meta-"

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

chunk_headers = {
    "container_id": "%scontainer-id" % CHUNK_METADATA_PREFIX,
    "chunk_id": "%schunk-id" % CHUNK_METADATA_PREFIX,
    "chunk_hash": "%schunk-hash" % CHUNK_METADATA_PREFIX,
    "chunk_size": "%schunk-size" % CHUNK_METADATA_PREFIX,
    "chunk_pos": "%schunk-pos" % CHUNK_METADATA_PREFIX,
    "content_size": "%scontent-size" % CHUNK_METADATA_PREFIX,
    "content_path": "%scontent-path" % CHUNK_METADATA_PREFIX,
    "content_chunksnb": "%scontent-chunksnb" % CHUNK_METADATA_PREFIX,
    "content_hash": "%scontent-hash" % CHUNK_METADATA_PREFIX
}


def handle_container_not_found(fnc):
    @wraps(fnc)
    def _wrapped(self, account, container, *args, **kwargs):
        try:
            return fnc(self, account, container, *args, **kwargs)
        except exc.NotFound as e:
            e.message = "Container '%s' does not exist." % container
            raise exc.NoSuchContainer(e)

    return _wrapped


def handle_object_not_found(fnc):
    @wraps(fnc)
    def _wrapped(self, account, container, obj, *args, **kwargs):
        try:
            return fnc(self, account, container, obj, *args, **kwargs)
        except exc.NotFound as e:
            e.message = "Object '%s' does not exist." % obj
            raise exc.NoSuchObject(e)

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
        endpoint_v3 = '/'.join([endpoint.rstrip('/'), 'v3.0'])
        super(ObjectStorageAPI, self).__init__(endpoint=endpoint_v3, **kwargs)
        self.directory = DirectoryAPI(
            namespace,
            endpoint,
            session=self.session
        )
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

    def account_set_properties(self, account, properties, headers=None):
        self.account_update(account, properties, headers=headers)

    def account_del_properties(self, account, properties, headers=None):
        self.account_update(account, None, properties, headers=headers)

    def container_create(self, account, container, headers=None):
        try:
            self.directory.link(account, container, "meta2", headers=headers)
        except exc.NotFound:
            self.directory.create(account, container, headers=headers)
            self.directory.link(account, container, "meta2", headers=headers)

        uri = self._make_uri('container/create')
        params = self._make_params(account, container)

        resp, resp_body = self._request(
            'POST', uri, params=params, headers=headers)
        if resp.status_code not in (204, 201):
            raise exc.from_response(resp, resp_body)
        if resp.status_code == 201:
            return False
        else:
            return True

    @handle_container_not_found
    def container_delete(self, account, container, headers=None):
        uri = self._make_uri('container/destroy')
        params = self._make_params(account, container)
        try:
            resp, resp_body = self._request(
                'POST', uri, params=params, headers=headers)
        except exc.Conflict as e:
            raise exc.ContainerNotEmpty(e)

        self.directory.unlink(account, container, "meta2", headers=headers)

    def container_list(self, account, limit=None, marker=None,
                       end_marker=None, prefix=None, delimiter=None,
                       headers=None):
        uri = "v1.0/account/containers"
        account_id = utils.quote(account, '')
        params = {"id": account_id, "limit": limit, "marker": marker,
                  "delimiter": delimiter, "prefix": prefix,
                  "end_marker": end_marker}

        resp, resp_body = self._account_request(
            'GET', uri, params=params, headers=headers)
        listing = resp_body['listing']
        del resp_body['listing']
        return listing, resp_body

    @handle_container_not_found
    def container_show(self, account, container, headers=None):
        uri = self._make_uri('container/get_properties')
        params = self._make_params(account, container)
        resp, resp_body = self._request(
            'POST', uri, params=params, headers=headers)
        return resp_body

    def container_update(self, account, container, metadata, clear=False,
                         headers=None):
        if not metadata:
            self.container_del_properties(
                account, container, [], headers=headers)
        else:
            self.container_set_properties(
                account, container, metadata, clear, headers=headers)

    @handle_container_not_found
    def container_set_properties(self, account, container, properties,
                                 clear=False, headers=None):
        params = self._make_params(account, container)

        if clear:
            params.update({'flush': 1})

        uri = self._make_uri('container/set_properties')

        resp, resp_body = self._request(
            'POST', uri, data=json.dumps(properties), params=params,
            headers=headers)

    @handle_container_not_found
    def container_del_properties(self, account, container, properties,
                                 headers=None):
        params = self._make_params(account, container)

        uri = self._make_uri('container/del_properties')

        resp, resp_body = self._request(
            'POST', uri, data=json.dumps(properties), params=params,
            headers=headers)

    @handle_container_not_found
    def object_create(self, account, container, file_or_path=None, data=None,
                      etag=None, obj_name=None, content_type=None,
                      content_encoding=None, content_length=None,
                      metadata=None, headers=None):
        if (data, file_or_path) == (None, None):
            raise exc.MissingData()
        src = data if data is not None else file_or_path
        if src is file_or_path:
            if isinstance(file_or_path, basestring):
                if not os.path.exists(file_or_path):
                    raise exc.FileNotFound("File '%s' not found." %
                                           file_or_path)
                file_name = os.path.basename(file_or_path)
            else:
                try:
                    file_name = os.path.basename(file_or_path.name)
                except AttributeError:
                    file_name = None
            obj_name = obj_name or file_name
        if not obj_name:
            raise exc.MissingName(
                "No name for the object has been specified"
            )

        if isinstance(data, basestring):
            content_length = len(data)

        if content_length is None:
            raise exc.MissingContentLength()

        sysmeta = {'content_type': content_type,
                   'content_encoding': content_encoding,
                   'content_length': content_length,
                   'etag': etag}

        if src is data:
            return self._object_create(
                account, container, obj_name, StringIO(data), sysmeta,
                headers=headers)
        elif hasattr(file_or_path, "read"):
            return self._object_create(
                account, container, obj_name, src, sysmeta, headers=headers)
        else:
            with open(file_or_path, "rb") as f:
                return self._object_create(
                    account, container, obj_name, f, sysmeta, headers=headers)

    @handle_object_not_found
    def object_delete(self, account, container, obj, headers=None):
        uri = self._make_uri('content/delete')
        params = self._make_params(account, container, obj)
        resp, resp_body = self._request(
            'POST', uri, params=params, headers=headers)

    @handle_container_not_found
    def object_list(self, account, container, limit=None, marker=None,
                    delimiter=None, prefix=None, end_marker=None,
                    headers=None):
        uri = self._make_uri('container/list')
        params = self._make_params(account, container)
        d = {"max": limit,
             "marker": marker,
             "delimiter": delimiter,
             "prefix": prefix,
             "end_marker": end_marker}
        params.update(d)

        resp, resp_body = self._request(
            'GET', uri, params=params, headers=headers)

        return resp_body

    @handle_object_not_found
    def object_fetch(self, account, container, obj, size=None, offset=0,
                     headers=None):
        uri = self._make_uri('content/show')
        params = self._make_params(account, container, obj)
        resp, resp_body = self._request(
            'GET', uri, params=params, headers=headers)

        meta = _make_object_metadata(resp.headers)
        raw_chunks = resp_body

        rain_security = len(raw_chunks[0]["pos"].split(".")) == 2
        chunks = _sort_chunks(raw_chunks, rain_security)
        stream = self._fetch_stream(meta, chunks, rain_security, size, offset,
                                    headers=headers)
        return meta, stream

    @handle_object_not_found
    def object_show(self, account, container, obj, headers=None):
        uri = self._make_uri('content/get_properties')
        params = self._make_params(account, container, obj)
        resp, resp_body = self._request(
            'POST', uri, params=params, headers=headers)

        meta = _make_object_metadata(resp.headers)
        for k, v in resp_body.iteritems():
            meta[k] = v
        meta['properties'] = resp_body
        return meta

    def object_update(self, account, container, obj, metadata, clear=False,
                      headers=None):
        if clear:
            self.object_del_properties(
                account, container, obj, [], headers=headers)
        if metadata:
            self.object_set_properties(
                account, container, obj, metadata, headers=headers)

    @handle_object_not_found
    def object_set_properties(self, account, container, obj, properties,
                              clear=False, headers=None):
        params = self._make_params(account, container, obj)
        if clear:
            params.update({'flush': 1})
        uri = self._make_uri('content/set_properties')
        resp, resp_body = self._request(
            'POST', uri, data=json.dumps(properties), params=params,
            headers=headers)

    @handle_object_not_found
    def object_del_properties(self, account, container, obj, properties,
                              headers=None):
        params = self._make_params(account, container, obj)
        uri = self._make_uri('content/del_properties')
        resp, resp_body = self._request(
            'POST', uri, data=json.dumps(properties), params=params,
            headers=headers)

    def _make_uri(self, action):
        uri = "%s/%s" % (self.namespace, action)
        return uri

    def _make_params(self, account, ref, obj=None):
        params = {'acct': account,
                  'ref': ref}
        if obj:
            params.update({'path': obj})
        return params

    def _get_account_url(self):
        uri = 'lb/%s/account' % self.namespace
        resp, resp_body = self._request('GET', uri)
        if resp.status_code == 200:
            instance_info = resp_body[0]
            return 'http://%s/' % instance_info['addr']
        else:
            raise exc.ClientException(
                "could not find account instance url"
            )

    def _account_request(self, method, uri, **kwargs):
        account_url = self._get_account_url()
        resp, resp_body = self._request(method, uri, endpoint=account_url,
                                        **kwargs)
        return resp, resp_body

    def _object_create(self, account, container, obj_name, src,
                       sysmeta, headers=None):
        uri = self._make_uri('content/prepare')
        params = self._make_params(account, container, obj_name)
        args = {'size': sysmeta['content_length']}
        resp, resp_body = self._request(
            'POST', uri, data=json.dumps(args), params=params,
            headers=headers)

        raw_chunks = resp_body

        rain_security = len(raw_chunks[0]["pos"].split(".")) == 2
        if rain_security:
            raise exc.OioException('RAIN Security not supported.')

        chunks = _sort_chunks(raw_chunks, rain_security)
        final_chunks, bytes_transferred, content_checksum = self._put_stream(
            account, container, obj_name, src, sysmeta, chunks,
            headers=headers)

        sysmeta['etag'] = content_checksum

        headers = {"x-oio-content-meta-length": bytes_transferred,
                   "x-oio-content-meta-hash": sysmeta['etag'],
                   "content-type": sysmeta['content_type']}

        uri = self._make_uri('content/create')
        data = json.dumps(final_chunks)
        resp, resp_body = self._request(
            'POST', uri, data=data, params=params, headers=headers)
        return final_chunks, bytes_transferred, content_checksum

    def _put_stream(self, account, container, obj_name, src, sysmeta, chunks,
                    headers=None):
        global_checksum = hashlib.md5()
        total_bytes_transferred = 0
        content_chunks = []

        def _connect_put(chunk):
            raw_url = chunk["url"]
            parsed = urlparse(raw_url)
            try:
                chunk_path = parsed.path.split('/')[-1]
                hdrs = {}
                hdrs["transfer-encoding"] = "chunked"
                hdrs[chunk_headers["content_path"]] = utils.quote(obj_name)
                hdrs[chunk_headers["content_size"]] = sysmeta['content_length']
                hdrs[chunk_headers["content_chunksnb"]] = len(chunks)
                hdrs[chunk_headers["container_id"]] = \
                    utils.name2cid(account, container)
                hdrs[chunk_headers["chunk_pos"]] = chunk["pos"]
                hdrs[chunk_headers["chunk_id"]] = chunk_path
                with ConnectionTimeout(CONNECTION_TIMEOUT):
                    conn = http_connect(
                        parsed.netloc, 'PUT', parsed.path, hdrs)
                    conn.chunk = chunk
                return conn
            except (Exception, Timeout):
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
                raise exc.OioException("RAWX connection failure")

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
                            raise exc.OioException("RAWX write failure")

                    for conn in conns:
                        if conn.queue.unfinished_tasks:
                            conn.queue.join()

                conns = [conn for conn in conns if not conn.failed]

            except ChunkReadTimeout:
                raise exc.ClientReadTimeout()
            except (Exception, Timeout):
                raise exc.OioException(
                    "Exception during chunk write."
                )

            final_chunks = []
            for conn in conns:
                resp = conn.getresponse()
                if resp.status in (200, 201):
                    conn.chunk["size"] = bytes_transferred
                    final_chunks.append(conn.chunk)
                conn.close()
            if len(final_chunks) < min_conns:
                raise exc.OioException("RAWX write failure")

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
            raise exc.OioException("RAIN not supported")
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
                        raise exc.OioException("Error while downloading")
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

            except Exception:
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
