import collections
import logging
import random

from tornado import gen, ioloop

from zoonado import protocol, exc
from .connection import Connection
from .states import States, SessionStateMachine
from .retry import RetryPolicy


DEFAULT_ZOOKEEPER_PORT = 2181

MAX_FIND_WAIT = 60  # in seconds

HEARTBEAT_FREQUENCY = 3  # heartbeats per timeout interval


log = logging.getLogger(__name__)


class Session(object):

    def __init__(self, servers, timeout, retry_policy, allow_read_only):
        self.hosts = []
        for server in servers.split(","):
            if ":" in server:
                host, port = server.split(":")
            else:
                host = server
                port = DEFAULT_ZOOKEEPER_PORT

            self.hosts.append((host, port))

        self.conn = None
        self.state = SessionStateMachine()

        self.retry_policy = retry_policy or RetryPolicy.forever()
        self.allow_read_only = allow_read_only

        self.xid = 0
        self.last_zxid = None

        self.session_id = None
        self.timeout = timeout
        self.password = b'\x00'

        self.heartbeat_handle = None

        self.watch_callbacks = collections.defaultdict(set)

        self.closing = False

    @gen.coroutine
    def ensure_safe_state(self, writing=False):
        safe_states = [States.CONNECTED]
        if self.allow_read_only and not writing:
            safe_states.append(States.READ_ONLY)

        if self.state in safe_states:
            return

        yield self.state.wait_for(*safe_states)

    @gen.coroutine
    def start(self):
        io_loop = ioloop.IOLoop.current()
        io_loop.add_callback(self.set_heartbeat)
        io_loop.add_callback(self.repair_loop)

        yield self.ensure_safe_state()

    @gen.coroutine
    def find_server(self, allow_read_only):
        conn = None

        retry_policy = RetryPolicy.exponential_backoff(maximum=MAX_FIND_WAIT)

        while not conn:
            yield retry_policy.enforce()

            servers = random.sample(self.hosts, len(self.hosts))
            for host, port in servers:
                log.info("Connecting to %s:%s", host, port)
                conn = yield self.make_connection(host, port)
                if not conn or (conn.start_read_only and not allow_read_only):
                    continue

            if not conn:
                log.warn("No servers available, will keep trying.")

        old_conn = self.conn
        self.conn = conn

        io_loop = ioloop.IOLoop.current()

        if old_conn:
            io_loop.add_callback(old_conn.close, self.timeout)

        if conn.start_read_only:
            io_loop.add_callback(self.find_server, allow_read_only=False)

    @gen.coroutine
    def make_connection(self, host, port):
        conn = Connection(host, port, watch_handler=self.event_dispatch)
        try:
            yield conn.connect()
        except Exception:
            log.exception("Couldn't connect to %s:%s", host, port)
            return

        raise gen.Return(conn)

    @gen.coroutine
    def establish_session(self):
        log.info("Establising session.")
        zxid, response = yield self.conn.send_connect(
            protocol.ConnectRequest(
                protocol_version=0,
                last_seen_zxid=self.last_zxid or 0,
                timeout=int((self.timeout or 0) * 1000),
                session_id=self.session_id or 0,
                password=self.password,
                read_only=self.allow_read_only,
            )
        )
        self.last_zxid = zxid

        if response.session_id == 0:  # invalid session, probably expired
            self.state.transition_to(States.LOST)
            raise exc.SessionLost()

        log.info("Got session id %s", hex(response.session_id))
        log.info("Negotiated timeout: %s seconds", response.timeout / 1000)

        self.session_id = response.session_id
        self.password = response.password
        self.timeout = response.timeout / 1000

        self.last_zxid = zxid

    @gen.coroutine
    def repair_loop(self):
        while not self.closing:
            yield self.state.wait_for(States.SUSPENDED, States.LOST)
            if self.closing:
                break

            yield self.find_server(allow_read_only=self.allow_read_only)

            session_was_lost = self.state == States.LOST

            try:
                yield self.establish_session()
            except exc.SessionLost:
                self.conn.abort(exc.SessionLost)
                yield self.conn.close()
                self.session_id = None
                self.password = b'\x00'
                continue

            if self.conn.start_read_only:
                self.state.transition_to(States.READ_ONLY)
            else:
                self.state.transition_to(States.CONNECTED)

            self.conn.start_read_loop()

            if session_was_lost:
                yield self.set_existing_watches()

    @gen.coroutine
    def send(self, request):
        response = None
        while not response:
            yield self.retry_policy.enforce(request)
            yield self.ensure_safe_state(writing=request.writes_data)

            try:
                self.xid += 1
                zxid, response = yield self.conn.send(request, xid=self.xid)
                self.last_zxid = zxid
                self.set_heartbeat()
                self.retry_policy.clear(request)
            except exc.ConnectError:
                self.state.transition_to(States.SUSPENDED)

        raise gen.Return(response)

    def set_heartbeat(self):
        timeout = self.timeout / HEARTBEAT_FREQUENCY

        io_loop = ioloop.IOLoop.current()

        if self.heartbeat_handle:
            io_loop.remove_timeout(self.heartbeat_handle)

        self.heartbeat_handle = io_loop.call_later(timeout, self.heartbeat)

    @gen.coroutine
    def heartbeat(self):
        if self.closing:
            return

        yield self.ensure_safe_state()

        try:
            zxid, _ = yield self.conn.send(protocol.PingRequest())
            self.last_zxid = zxid
        except exc.ConnectError:
            self.state.transition_to(States.SUSPENDED)
        finally:
            self.set_heartbeat()

    def add_watch_callback(self, event_type, path, callback):
        self.watch_callbacks[(event_type, path)].add(callback)

    def remove_watch_callback(self, event_type, path, callback):
        self.watch_callbacks[(event_type, path)].discard(callback)

    def event_dispatch(self, event):
        log.debug("Got watch event: %s", event)

        if event.type:
            key = (event.type, event.path)
            for callback in self.watch_callbacks[key]:
                ioloop.IOLoop.current().add_callback(callback, event.path)
            return

        if event.state == protocol.WatchEvent.DISCONNECTED:
            log.error("Got 'disconnected' watch event.")
            self.state.transition_to(States.LOST)
        elif event.state == protocol.WatchEvent.SESSION_EXPIRED:
            log.error("Got 'session expired' watch event.")
            self.state.transition_to(States.LOST)
        elif event.state == protocol.WatchEvent.AUTH_FAILED:
            log.error("Got 'auth failed' watch event.")
            self.state.transition_to(States.LOST)
        elif event.state == protocol.WatchEvent.CONNECTED_READ_ONLY:
            log.warn("Got 'connected read only' watch event.")
            self.state.transition_to(States.READ_ONLY)
        elif event.state == protocol.WatchEvent.SASL_AUTHENTICATED:
            log.info("Authentication successful.")
        elif event.state == protocol.WatchEvent.CONNECTED:
            log.info("Got 'connected' watch event.")
            self.state.transition_to(States.CONNECTED)

    @gen.coroutine
    def set_existing_watches(self):
        if not self.watch_callbacks:
            return

        request = protocol.SetWatchesRequest(
            relative_zxid=self.last_zxid or 0,
            data_watches=[],
            exist_watches=[],
            child_watches=[],
        )

        for event_type, path in self.watch_callbacks.keys():
            if event_type == protocol.WatchEvent.CREATED:
                request.exist_watches.append(path)
            if event_type == protocol.WatchEvent.DATA_CHANGED:
                request.data_watches.append(path)
            elif event_type == protocol.WatchEvent.CHILDREN_CHANGED:
                request.child_watches.append(path)

        yield self.send(request)

    @gen.coroutine
    def close(self):
        self.closing = True

        yield self.send(protocol.CloseRequest())
        self.state.transition_to(States.LOST)

        yield self.conn.close(self.timeout)
