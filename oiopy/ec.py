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

import collections
import math
import hashlib
import logging
from urlparse import urlparse
from eventlet import Queue, Timeout, GreenPile
from oiopy import utils
from greenlet import GreenletExit
from oiopy import exceptions as exc
from oiopy.exceptions import ChunkReadTimeout, ChunkWriteTimeout, \
    ConnectionTimeout
from oiopy.http import http_connect, parse_content_range
from oiopy import io
from oiopy.constants import chunk_headers


logger = logging.getLogger(__name__)


def segment_range_to_fragment_range(segment_start, segment_end, segment_size,
                                    fragment_size):
    """
    Converts a segment range into a fragment range.

    :returns: a tuple (fragment_start, fragment_end)

        * fragment_start is the first byte of the first fragment,
          or None if this is a suffix byte range

        * fragment_end is the last byte of the last fragment,
          or None if this is a prefix byte range
    """
    fragment_start = ((segment_start / segment_size * fragment_size)
                      if segment_start is not None else None)

    fragment_end = (None if segment_end is None else
                    ((segment_end + 1) / segment_size * fragment_size)
                    if segment_start is None else
                    ((segment_end + 1) / segment_size * fragment_size) - 1)

    return (fragment_start, fragment_end)


def obj_range_to_meta_chunk_range(obj_start, obj_end, meta_sizes):
    """
    Converts a requested object range into a list of meta_chunk ranges.

    Examples:

    TODO complete examples
    (20, 150, [50, 50]) = {0: (20, None), 1: (None, None)}
    (20, 150, [50, 100]) = {0: (20, None), 1: (None, 100)}
    (150, None, [100, 100]) = {1: (50, None)}

    :returns: a tuple list (pos, meta_chunk_start, meta_chunk_end)

        * pos is the meta chunk position

        * meta_chunk_start is the first byte of the meta chunk,
          or None if this is a suffix byte range

        * meta_chunk_end is the last byte of the meta_chunk,
          or None if this is a prefix byte range
    """

    offset = 0
    found_start = False
    found_end = False
    meta_chunk_ranges = {}
    for pos, meta_size in enumerate(meta_sizes):
        if found_start:
            meta_chunk_start = None
        elif obj_start is not None and obj_start > offset + meta_size:
            offset += meta_size
            continue
        elif obj_start is not None and obj_start <= offset + meta_size:
            meta_chunk_start = obj_start - offset
            found_start = True
        else:
            meta_chunk_start = None
        if obj_end is not None and offset + meta_size >= obj_end:
            meta_chunk_end = obj_end - offset
            # found end
            found_end = True
        else:
            meta_chunk_end = None
        meta_chunk_ranges[pos] = (meta_chunk_start, meta_chunk_end)
        if found_end:
            break
        offset += meta_size

    return meta_chunk_ranges


def meta_chunk_range_to_segment_range(meta_start, meta_end, segment_size):
    """
    Converts a meta chunk range to a segment range.

    Examples:
        meta_chunk_range_to_segment_range(100, 600, 256) = (0, 767)
        meta_chunk_range_to_segment_range(100, 600, 512) = (0, 1023)
        meta_chunk_range_to_segment_range(300, None, 256) = (256, None)

    :returns: a typle (segment_start, segment_end)

        * segment_start is the first byte of the first segment,
          or None if suffix byte range

        * segment_end is the last byte of the last segment,
          or None if prefix byte range

    """

    segment_start = (int(meta_start // segment_size) *
                     segment_size) if meta_start is not None else None
    segment_end = (None if meta_end is None else
                   (((int(meta_end // segment_size) + 1) *
                     segment_size) - 1) if meta_start is not None else
                   (int(math.ceil((float(meta_end) / segment_size) + 1)) *
                       segment_size))
    return (segment_start, segment_end)


class ECChunkDownloadHandler(object):
    """
    Handles the download of an EC meta chunk
    """
    def __init__(self, storage_method, chunks, meta_start, meta_end, headers):
        self.storage_method = storage_method
        self.chunks = chunks
        self.meta_start = meta_start
        self.meta_end = meta_end
        self.headers = headers

    def _get_range_infos(self):
        """
        Converts requested Range on meta chunk to actual chunk Range

        :returns: a dict with infos about all the requested Ranges
        """
        segment_size = self.storage_method.ec_segment_size
        fragment_size = self.storage_method.ec_fragment_size

        range_infos = []

        # read all the meta chunk
        if self.meta_start is None and self.meta_end is None:
            return range_infos

        segment_start, segment_end = meta_chunk_range_to_segment_range(
            self.meta_start, self.meta_end, segment_size)

        fragment_start, fragment_end = segment_range_to_fragment_range(
            segment_start, segment_end, segment_size, fragment_size)

        range_infos.append({
            'req_meta_start': self.meta_start,
            'req_meta_end': self.meta_end,
            'req_segment_start': segment_start,
            'req_segment_end': segment_end,
            'req_fragment_start': fragment_start,
            'req_fragment_end': fragment_end})
        return range_infos

    def _get_fragment(self, chunk_iter, storage_method):
        # TODO generate proper headers
        headers = {}
        reader = io.ChunkReader(chunk_iter, storage_method.ec_fragment_size,
                                headers)
        return (reader, reader.get_iter())

    def get_stream(self):
        range_infos = self._get_range_infos()

        # the meta chunk length
        # (the amount of actual data stored into the meta chunk)
        meta_length = self.chunks[0]['size']
        chunk_iter = iter(self.chunks)

        # we use eventlet GreenPool to manage readers
        with utils.ContextPool(self.storage_method.ec_nb_data) as pool:
            pile = GreenPile(pool)
            # we use eventlet GreenPile to spawn readers
            for _j in range(self.storage_method.ec_nb_data):
                pile.spawn(self._get_fragment, chunk_iter, self.storage_method)

            readers = []
            for reader, parts_iter in pile:
                if reader.status in (200, 206):
                    readers.append((reader, parts_iter))
                # TODO log failures?

        # with EC we need at least ec_nb_data valid readers
        if len(readers) >= self.storage_method.ec_nb_data:
            # all readers should return the same Content-Length
            # so just take the headers from one of them
            resp_headers = utils.HeadersDict(readers[0][0].headers)
            fragment_length = int(resp_headers.get('Content-Length'))
            r = [it for reader, it in readers]
            stream = ECStream(self.storage_method, r, range_infos,
                              meta_length, fragment_length)
            # start the stream
            stream.start()
            return stream
        else:
            raise exc.OioException("Not enough valid sources to read")


class ECStream(object):
    """
    Reads an EC meta chunk.

    Handles the different readers.
    """
    def __init__(self, storage_method, readers, range_infos, meta_length,
                 fragment_length):
        self.storage_method = storage_method
        self.readers = readers
        self.range_infos = range_infos
        self.meta_length = meta_length
        self.fragment_length = fragment_length

    def start(self):
        self._iter = io.chain(self._stream())

    def close(self):
        if self._iter:
            self._iter.close()
        for reader in self.readers:
            reader.close()

    def _next(self):
        fragment_iterators = []
        for iterator in self.readers:
            part_info = next(iterator)
            fragment_iterators.append(part_info['iter'])
            headers = utils.HeadersDict(part_info['headers'])
        return headers, fragment_iterators

    def _iter_range(self, range_info, segment_iter):
        meta_start = range_info['resp_meta_start']
        meta_end = range_info['resp_meta_end']
        segment_start = range_info['resp_segment_start']
        segment_end = range_info['resp_segment_end']

        segment_end = (min(segment_end, self.meta_length - 1)
                       if segment_end is not None
                       else self.meta_length - 1)
        meta_end = (min(meta_end, self.meta_length - 1)
                    if meta_end is not None
                    else self.meta_length - 1)

        num_segments = int(
            math.ceil(float(segment_end + 1 - segment_start) /
                      self.storage_method.ec_segment_size))

        # we read full segments from the chunks
        # however we may be requested a byte range
        # that is not aligned with the segments
        # so we read and trim extra bytes from the segment
        start_over = meta_start - segment_start
        end_over = segment_end - meta_end

        for i, segment in enumerate(segment_iter):
            if start_over > 0:
                segment_len = len(segment)
                if segment_len <= start_over:
                    start_over -= segment_len
                    continue
                else:
                    segment = segment[start_over:]
                    start_over = 0
            if i == (num_segments - 1) and end_over:
                segment = segment[:-end_over]

            yield segment

    def _decode_segments(self, fragment_iterators):
        """
        Reads from fragments and yield full segments
        """
        # we use eventlet Queue to read fragments
        queues = []
        # each iterators has its queue
        for _j in range(len(fragment_iterators)):
            queues.append(Queue(1))

        def put_in_queue(fragment_iterator, queue):
            """
            Coroutine to read the fragments from the iterator
            """
            try:
                for fragment in fragment_iterator:
                    # put the read fragment in the queue
                    queue.put(fragment)
                    # the queues are of size 1 so this coroutine blocks
                    # until we decode a full segment
            except GreenletExit:
                # ignore
                pass
            except ChunkReadTimeout:
                logger.exception("Timeout on reading")
            except:
                logger.exception("Exception on reading")
            finally:
                queue.resize(2)
                # put None to indicate the decoding loop
                # this is over
                queue.put(None)
                # close the iterator
                fragment_iterator.close()

        # we use eventlet GreenPool to manage the read of fragments
        with utils.ContextPool(len(fragment_iterators)) as pool:
            # spawn coroutines to read the fragments
            for fragment_iterator, queue in zip(fragment_iterators, queues):
                pool.spawn(put_in_queue, fragment_iterator, queue)

            # main decoding loop
            while True:
                data = []
                # get the fragments from the queues
                for queue in queues:
                    fragment = queue.get()
                    queue.task_done()
                    data.append(fragment)

                if not all(data):
                    # one of the readers returned None
                    # impossible to read segment
                    break
                # actually decode the fragments into a segment
                try:
                    segment = self.storage_method.driver.decode(data)
                except exc.ECError:
                    # something terrible happened
                    logger.exception("ERROR decoding fragments")
                    raise

                yield segment

    def _convert_range(self, req_start, req_end, length):
        try:
            ranges = utils.ranges_from_http_header("bytes=%s-%s" % (
                req_start if req_start is not None else '',
                req_end if req_end is not None else ''))
        except ValueError:
            return (None, None)

        result = utils.convert_ranges(ranges, length)
        if not result:
            return (None, None)
        else:
            return (result[0][0], result[0][1])

    def _add_ranges(self, range_infos):
        for range_info in range_infos:
            meta_start, meta_end = self._convert_range(
                range_info['req_meta_start'], range_info['req_meta_end'],
                self.meta_length)
            range_info['resp_meta_start'] = meta_start
            range_info['resp_meta_end'] = meta_end
            range_info['satisfiable'] = \
                (meta_start is not None and meta_end is not None)

            segment_start, segment_end = self._convert_range(
                range_info['req_segment_start'], range_info['req_segment_end'],
                self.meta_length)

            segment_size = self.storage_method.ec_segment_size

            if range_info['req_segment_start'] is None and \
                    segment_start % segment_size != 0:
                segment_start += segment_start - (segment_start % segment_size)

            range_info['resp_segment_start'] = segment_start
            range_info['resp_segment_end'] = segment_end

    def _add_ranges_for_fragment(self, fragment_length, range_infos):
        for range_info in range_infos:
            fragment_start, fragment_end = self._convert_range(
                range_info['req_fragment_start'],
                range_info['req_fragment_end'],
                fragment_length)

    def _stream(self):
        if not self.range_infos:
            range_infos = [{
                'req_meta_start': 0,
                'req_meta_end': self.meta_length - 1,
                'resp_meta_start': 0,
                'resp_meta_end': self.meta_length - 1,
                'req_segment_start': 0,
                'req_segment_end': self.meta_length - 1,
                'req_fragment_start': 0,
                'req_fragment_end': self.fragment_length - 1,
                'resp_fragment_start': 0,
                'resp_fragment_end': self.fragment_length - 1,
                'satisfiable': self.meta_length > 0
            }]

        else:
            range_infos = self.range_infos

        self._add_ranges(range_infos)

        def range_iter():
            results = {}

            while True:
                next_range = self._next()

                headers, fragment_iters = next_range
                content_range = headers.get('Content-Range')
                if content_range is not None:
                    fragment_start, fragment_end, fragment_length = \
                            parse_content_range(content_range)
                else:
                    fragment_start = 0
                    fragment_end = self.fragment_length - 1
                    fragment_length = self.fragment_length

                self._add_ranges_for_fragment(fragment_length, range_infos)

                satisfiable = False

                for range_info in range_infos:
                    satisfiable |= range_info['satisfiable']
                    k = (range_info['resp_fragment_start'],
                         range_info['resp_fragment_end'])
                    results.setdefault(k, []).append(range_info)

                range_info = results[(fragment_start, fragment_end)].pop(0)
                segment_iter = self._decode_segments(fragment_iters)

                if not range_info['satisfiable']:
                    io.consume(segment_iter)
                    continue

                byterange_iter = self._iter_range(range_info, segment_iter)

                result = {'start': range_info['resp_meta_start'],
                          'end': range_info['resp_meta_end'],
                          'iter': byterange_iter}

                yield result

        return range_iter()

    def __iter__(self):
        return iter(self._iter)

    def get_iter(self):
        return self


def ec_encode(storage_method, n):
    """
    Encode EC segments
    """
    segment_size = storage_method.ec_segment_size

    buf = collections.deque()
    total_len = 0

    data = yield
    while data:
        buf.append(data)
        total_len += len(data)

        if total_len >= segment_size:
            data_to_encode = []

            while total_len >= segment_size:
                # take data from buf
                amount = segment_size
                # the goal here is to encode a full segment
                parts = []
                while amount > 0:
                    part = buf.popleft()
                    if len(part) > amount:
                        # too much data taken
                        # put the extra data back into the buf
                        buf.appendleft(part[amount:])
                        part = part[:amount]
                    parts.append(part)
                    amount -= len(part)
                    total_len -= len(part)
                data_to_encode.append(''.join(parts))

            # let's encode!
            encode_result = []
            for d in data_to_encode:
                encode_result.append(storage_method.driver.encode(d))

            # transform the result
            #
            # from:
            # [[fragment_0_0, fragment_1_0, fragment_2_0, ...],
            #  [fragment_0_1, fragment_1_1, fragment_2_1, ...], ...]
            #
            # to:
            #
            # [(fragment_0_0 + fragment_0_1 + ...), # write to chunk 0
            # [(fragment_1_0 + fragment_1_1 + ...), # write to chunk 1
            # [(fragment_2_0 + fragment_2_1 + ...), # write to chunk 2
            #  ...]

            result = [''.join(p) for p in zip(*encode_result)]
            data = yield result
        else:
            # not enough data to encode
            data = yield None

    # empty input data
    # which means end of stream
    # encode what is left in the buf
    whats_left = ''.join(buf)
    if whats_left:
        last_fragments = storage_method.driver.encode(whats_left)
        yield last_fragments
    else:
        yield [''] * n


class ECWriter(object):
    """
    Writes an EC chunk
    """
    def __init__(self, chunk, conn):
        self._chunk = chunk
        self._conn = conn
        self.failed = False
        self.bytes_transferred = 0
        self.checksum = hashlib.md5()

    @property
    def chunk(self):
        return self._chunk

    @property
    def conn(self):
        return self._conn

    @classmethod
    def connect(cls, chunk, sysmeta):
        raw_url = chunk["url"]
        parsed = urlparse(raw_url)
        try:
            chunk_path = parsed.path.split('/')[-1]
            h = {}
            h["transfer-encoding"] = "chunked"
            h[chunk_headers["content_id"]] = sysmeta['id']
            h[chunk_headers["content_version"]] = sysmeta['version']
            h[chunk_headers["content_path"]] = \
                utils.quote(sysmeta['content_path'])
            h[chunk_headers["content_size"]] = sysmeta['content_length']
            h[chunk_headers["content_chunkmethod"]] = sysmeta['chunk_method']
            h[chunk_headers["content_mimetype"]] = sysmeta['mime_type']
            h[chunk_headers["content_policy"]] = sysmeta['policy']
            h[chunk_headers["content_chunksnb"]] = sysmeta['content_chunksnb']
            h[chunk_headers["container_id"]] = sysmeta['container_id']
            h[chunk_headers["chunk_pos"]] = chunk["pos"]
            h[chunk_headers["chunk_id"]] = chunk_path
            with ConnectionTimeout(io.CONNECTION_TIMEOUT):
                conn = http_connect(
                    parsed.netloc, 'PUT', parsed.path, h)
                conn.chunk = chunk
            return cls(chunk, conn)
        except (Exception, Timeout):
            logger.exception("Failed to connect to %s", chunk)

    def start(self, pool):
        # we use eventlet Queue to pass data to the send coroutine
        self.queue = Queue(io.PUT_QUEUE_DEPTH)
        # spawn the send coroutine
        pool.spawn(self._send)

    def _send(self):
        # this is the send coroutine loop
        while True:
            # fetch input data from the queue
            d = self.queue.get()
            # use HTTP transfer encoding chunked
            # to write data to RAWX
            if not self.failed:
                if len(d) == 0:
                    # end of input
                    # finish the transfer with a zero length chunk
                    to_send = "0\r\n\r\n"
                else:
                    # format the chunk
                    to_send = "%x\r\n%s\r\n" % (len(d), d)
                try:
                    with ChunkWriteTimeout(io.CHUNK_TIMEOUT):
                        self.conn.send(to_send)
                        self.bytes_transferred += len(d)
                except (Exception, ChunkWriteTimeout):
                    self.failed = True
                    logger.exception("Failed to write to %s", self.chunk)

            self.queue.task_done()

    def wait(self):
        # wait until all data in the queue
        # has been processed by the send coroutine
        if self.queue.unfinished_tasks:
            self.queue.join()

    def send(self, data):
        # put the data to send into the queue
        # it will be processed by the send coroutine
        self.queue.put(data)

    def getresponse(self):
        # read the HTTP response from the connection
        with Timeout(io.CHUNK_TIMEOUT):
            self.resp = self.conn.getresponse(True)
            return self.resp


class ECChunkWriteHandler(object):
    def __init__(self, sysmeta, meta_chunk, checksum, storage_method):
        self.sysmeta = sysmeta
        self.meta_chunk = meta_chunk
        self.checksum = checksum
        self.storage_method = storage_method

    def stream(self, source, size):
        writers = self._get_writers()
        # write the data
        bytes_transferred = self._stream(source, size, writers)

        # remove failed writers
        writers = [w for w in writers if not w.failed]

        # get the final chunks from writers
        chunks = self._get_results(writers)
        meta_checksum = self.checksum.hexdigest()

        return bytes_transferred, meta_checksum, chunks

    def _stream(self, source, size, writers):
        bytes_transferred = 0

        # create EC encoding generator
        ec_stream = ec_encode(self.storage_method, len(writers))
        # init generator
        ec_stream.send(None)

        def send(data):
            self.checksum.update(data)
            # get the encoded fragments
            fragments = ec_stream.send(data)
            if fragments is None:
                # not enough data given
                return

            for writer in list(writers):
                fragment = fragments[chunk_index[writer]]
                if not writer.failed:
                    writer.checksum.update(fragment)
                    writer.send(fragment)
                else:
                    writers.remove(writer)
            self._check_quorum(writers)

        try:
            # we use eventlet GreenPool to manage writers
            with utils.ContextPool(len(writers)) as pool:
                # convenient index to figure out which writer
                # handles the resulting fragments
                chunk_index = self._build_index(writers)

                # init writers in pool
                for writer in writers:
                    writer.start(pool)

                # the main write loop
                while True:
                    remaining_bytes = size - bytes_transferred
                    if io.WRITE_CHUNK_SIZE < remaining_bytes:
                        read_size = io.WRITE_CHUNK_SIZE
                    else:
                        read_size = remaining_bytes
                    with ChunkReadTimeout(io.CLIENT_TIMEOUT):
                        data = source.read(read_size)
                    if len(data) == 0:
                        break
                    bytes_transferred += len(data)
                    send(data)

                # flush out any buffered data
                send('')

                # finish writing
                for writer in writers:
                    writer.send('')

                # wait for all data to be processed
                for writer in writers:
                    writer.wait()
                    writer.chunk['size'] = bytes_transferred

                return bytes_transferred

        except ChunkReadTimeout:
            # TODO
            raise
        except Timeout:
            # TODO
            raise
        except Exception:
            # TODO
            raise

    def _get_writers(self):
        # init writers to the chunks
        pile = GreenPile(len(self.meta_chunk))

        # we use eventlet GreenPile to spawn the writers
        for pos, chunk in enumerate(self.meta_chunk):
            pile.spawn(self._get_writer, chunk)

        # remove None results in pile
        writers = [w for w in pile if w]
        return writers

    def _get_writer(self, chunk):
        # spawn writer
        try:
            writer = ECWriter.connect(chunk, self.sysmeta)
            return writer
        except (Exception, Timeout):
            logger.exception("Failed to connect to %s", chunk)

    def _get_results(self, writers):
        # get the results from writers
        # we return the final chunks
        final_chunks = []

        # we use eventlet GreenPile to read the responses from the writers
        pile = GreenPile(len(writers))

        for writer in writers:
            if writer.failed:
                continue
            pile.spawn(self._get_response, writer)

        def _handle_resp(writer, resp):
            resp.read()
            if resp.status == 201:
                # TODO check checksum
                writer.chunk['hash'] = writer.checksum.hexdigest()
                final_chunks.append(writer.chunk)
            else:
                writer.failed = True
                logger.error("Wrong status code from %s (%s)",
                             writer.chunk, resp.status)

        for (writer, resp) in pile:
            if resp:
                _handle_resp(writer, resp)
            else:
                writer.failed = True

        # TODO check quorum on responses
        return final_chunks

    def _get_response(self, writer):
        # spawned in a coroutine to read the HTTP response
        try:
            resp = writer.getresponse()
        except (Exception, Timeout):
            resp = None
            logger.exception("Failed to read response %s", writer.chunk)
        return (writer, resp)

    def _build_index(self, writers):
        chunk_index = {}
        for w in writers:
            chunk_index[w] = w.chunk['num']
        return chunk_index

    def _check_quorum(self, writers):
        if len(writers) < self.storage_method.quorum:
            raise exc.OioException("Not enough valid RAWX connections")


class ECWriteHandler(io.WriteHandler):
    """
    Handles write to an EC content
    """
    def stream(self):
        # the checksum context for the content
        global_checksum = hashlib.md5()
        total_bytes_transferred = 0
        content_chunks = []

        # the platform chunk size
        chunk_size = self.sysmeta['chunk_size']

        # TODO is that lazy? :D
        # this gives us an upper bound
        max_size = self.storage_method.ec_nb_data * chunk_size
        max_size = max_size - max_size % self.storage_method.ec_segment_size

        # meta chunks:
        #
        # {0: [{"url": "http://...", "pos": "0.0"},
        #      {"url": "http://...", "pos": "0.1"}, ...],
        #  1: [{"url": "http://...", "pos": "1.0"},
        #      {"url": "http://...", "pos": "1.1"}, ...],
        #  ..}
        #
        # iterate through the meta chunks
        for pos in xrange(len(self.chunks)):
            meta_chunk = self.chunks[pos]

            handler = ECChunkWriteHandler(self.sysmeta, meta_chunk,
                                          global_checksum,
                                          self.storage_method)
            bytes_transferred, checksum, chunks = handler.stream(self.source,
                                                                 max_size)

            total_bytes_transferred += bytes_transferred
            # add the chunks to the content chunk list
            content_chunks += chunks

        # compute the final content checksum
        content_checksum = global_checksum.hexdigest()

        return content_chunks, total_bytes_transferred, content_checksum
