import collections
import logging
import re
import struct
import sys

from tornado import ioloop, iostream, gen, concurrent, tcpclient

from zoonado import protocol, iterables, exc


version_regex = re.compile(r'Zookeeper version: (\d)\.(\d)\.(\d)-.*')

# all requests and responses are prefixed with a 32-bit int denoting size
size_struct = struct.Struct("!i")
# replies are prefixed with an xid, zxid and error code
reply_header_struct = struct.Struct("!iqi")

log = logging.getLogger(__name__)
payload_log = logging.getLogger(__name__ + ".payload")
if payload_log.level == logging.NOTSET:
    payload_log.setLevel(logging.INFO)


class Connection(object):

    def __init__(self, host, port, watch_handler):
        self.host = host
        self.port = int(port)

        self.stream = None
        self.closing = False

        self.version_info = None
        self.start_read_only = None

        self.watch_handler = watch_handler

        self.opcode_xref = {}

        self.pending = {}
        self.pending_specials = collections.defaultdict(list)

        self.watches = collections.defaultdict(list)

    @gen.coroutine
    def connect(self):
        client = tcpclient.TCPClient()

        log.debug("Initial connection to server %s:%d", self.host, self.port)
        stream = yield client.connect(self.host, self.port)

        log.debug("Sending 'srvr' command to %s:%d", self.host, self.port)
        yield stream.write("srvr")
        answer = yield stream.read_until_close()

        version_line = answer.split("\n")[0]
        self.version_info = tuple(
            map(int, version_regex.match(version_line).groups())
        )
        self.start_read_only = bool("READ_ONLY" in answer)

        log.debug("Version info: %s", self.version_info)
        log.debug("Read-only mode: %s", self.start_read_only)

        log.debug("Actual connection to server %s:%d", self.host, self.port)
        self.stream = yield client.connect(self.host, self.port)

    @gen.coroutine
    def send_connect(self, request):
        # meant to be used before the read_loop starts
        payload_log.debug("[SEND] (initial) %s", request)

        payload = request.serialize()
        payload = size_struct.pack(len(payload)) + payload

        yield self.stream.write(payload)

        try:
            _, zxid, response = yield self.read_response(initial_connect=True)
        except Exception:
            log.exception("Error reading connect response.")
            return

        payload_log.debug("[RECV] (initial) %s", response)

        raise gen.Return((zxid, response))

    def start_read_loop(self):
        ioloop.IOLoop.current().add_callback(self.read_loop)

    def send(self, request, xid=None):
        f = concurrent.Future()

        if self.closing:
            f.set_exception(exc.ConnectError(self.host, self.port))
            return f

        if request.special_xid:
            xid = request.special_xid

        payload_log.debug("[SEND] (xid: %s) %s", xid, request)

        payload = request.serialize(xid)
        payload = size_struct.pack(len(payload)) + payload

        self.opcode_xref[xid] = request.opcode

        if xid in protocol.SPECIAL_XIDS:
            self.pending_specials[xid].append(f)
        else:
            self.pending[xid] = f

        def handle_write(write_future):
            try:
                write_future.result()
            except Exception:
                self.abort()

        try:
            self.stream.write(payload).add_done_callback(handle_write)
        except Exception:
            self.abort()

        return f

    @gen.coroutine
    def read_loop(self):
        """
        Infinite loop that reads messages off of the socket while not closed.

        When a message is received its corresponding pending Future is set
        to have the message as its result.

        This is never used directly and is fired as a separate callback on the
        I/O loop via the `connect()` method.
        """
        while not self.closing:
            try:
                xid, zxid, response = yield self.read_response()
            except iostream.StreamClosedError:
                return
            except Exception:
                log.exception("Error reading response.")
                self.abort()
                return

            payload_log.debug("[RECV] (xid: %s) %s", xid, response)

            if xid == protocol.WATCH_XID:
                self.watch_handler(response)
                continue
            elif xid in protocol.SPECIAL_XIDS:
                f = self.pending_specials[xid].pop()
            else:
                f = self.pending.pop(xid)

            if isinstance(response, Exception):
                f.set_exception(response)
            else:
                f.set_result((zxid, response))

    @gen.coroutine
    def read_response(self, initial_connect=False):
        raw_size = yield self.stream.read_bytes(size_struct.size)
        size = size_struct.unpack(raw_size)[0]

        # connect and close op replies don't contain a reply header
        if initial_connect or self.pending_specials[protocol.CLOSE_XID]:
            raw_payload = yield self.stream.read_bytes(size)
            response = protocol.ConnectResponse.deserialize(raw_payload)
            raise gen.Return((None, None, response))

        raw_header = yield self.stream.read_bytes(reply_header_struct.size)
        xid, zxid, error_code = reply_header_struct.unpack_from(raw_header)

        if error_code:
            raise gen.Return((xid, zxid, exc.get_response_error(error_code)))

        size -= reply_header_struct.size

        raw_payload = yield self.stream.read_bytes(size)

        if xid == protocol.WATCH_XID:
            response = protocol.WatchEvent.deserialize(raw_payload)
        else:
            opcode = self.opcode_xref.pop(xid)
            response = protocol.response_xref[opcode].deserialize(raw_payload)

        raise gen.Return((xid, zxid, response))

    def abort(self, exception=exc.ConnectError):
        """
        Aborts a connection and puts all pending futures into an error state.

        If ``sys.exc_info()`` is set (i.e. this is being called in an exception
        handler) then pending futures will have that exc info set.  Otherwise
        the given ``exception`` parameter is used (defaults to
        ``ConnectError``).
        """
        log.warn("Aborting connection to %s:%s", self.host, self.port)

        def abort_pending(f):
            exc_info = sys.exc_info()
            if any(exc_info):
                f.set_exc_info(exc_info)
            else:
                f.set_exception(exception(self.host, self.port))

        for pending in self.drain_all_pending():
            abort_pending(pending)

    def drain_all_pending(self):
        for special_xid in protocol.SPECIAL_XIDS:
            for _, f in iterables.drain(self.pending_specials[special_xid]):
                yield f
        for _, f in iterables.drain(self.pending):
            yield f

    @gen.coroutine
    def close(self, timeout):
        if self.closing:
            return

        self.closing = True

        pending_with_timeouts = []
        for pending in self.drain_all_pending():
            pending_with_timeouts.append(gen.with_timeout(timeout, pending))

        try:
            yield list(pending_with_timeouts)
        except gen.TimeoutError:
            yield self.abort(exception=exc.TimeoutError)
        finally:
            self.stream.close()
