import logging

from tornado import gen, concurrent

from zoonado import protocol, exc

from .recipes.proxy import RecipeProxy
from .session import Session
from .transaction import Transaction
from .features import Features


log = logging.getLogger(__name__)


class Zoonado(object):

    def __init__(
            self,
            servers,
            chroot=None,
            session_timeout=10,
            default_acl=None,
            retry_policy=None,
            allow_read_only=False,
    ):
        self.chroot = None
        if chroot:
            self.chroot = self.normalize_path(chroot)
            log.info("Using chroot '%s'", self.chroot)

        self.session = Session(
            servers, session_timeout, retry_policy, allow_read_only
        )

        self.default_acl = default_acl or [protocol.UNRESTRICTED_ACCESS]

        self.stat_cache = {}

        self.recipes = RecipeProxy(self)

    def normalize_path(self, path):
        if self.chroot:
            path = "/".join([self.chroot, path])

        normalized = "/".join([
            name for name in path.split("/")
            if name
        ])

        return "/" + normalized

    def denormalize_path(self, path):
        if self.chroot and path.startswith(self.chroot):
            path = path[len(self.chroot):]

        return path

    @gen.coroutine
    def start(self):
        yield self.session.start()

        if self.chroot:
            yield self.ensure_path("/")

    @property
    def features(self):
        if self.session.conn:
            return Features(self.session.conn.version_info)
        else:
            return Features((0, 0, 0))

    @gen.coroutine
    def send(self, request):
        response = yield self.session.send(request)

        if getattr(request, "path", None) and getattr(response, "stat", None):
            self.stat_cache[
                self.denormalize_path(request.path)
            ] = response.stat

        raise gen.Return(response)

    @gen.coroutine
    def close(self):
        yield self.session.close()

    def wait_for_event(self, event_type, path):
        path = self.normalize_path(path)

        f = concurrent.Future()

        def set_future(_):
            if not f.done():
                f.set_result(None)
            self.session.remove_watch_callback(event_type, path, set_future)

        self.session.add_watch_callback(event_type, path, set_future)

        return f

    @gen.coroutine
    def exists(self, path, watch=False):
        path = self.normalize_path(path)

        try:
            yield self.send(protocol.ExistsRequest(path=path, watch=watch))
        except exc.NoNode:
            raise gen.Return(False)

        raise gen.Return(True)

    @gen.coroutine
    def create(
            self, path, data=None, acl=None,
            ephemeral=False, sequential=False, container=False,
    ):
        if container and not self.features.containers:
            raise ValueError("Cannot create container, feature unavailable.")

        path = self.normalize_path(path)
        acl = acl or self.default_acl

        if self.features.create_with_stat:
            request_class = protocol.Create2Request
        else:
            request_class = protocol.CreateRequest

        request = request_class(path=path, data=data, acl=acl)
        request.set_flags(ephemeral, sequential, container)

        response = yield self.send(request)

        raise gen.Return(self.denormalize_path(response.path))

    @gen.coroutine
    def ensure_path(self, path, acl=None):
        path = self.normalize_path(path)

        acl = acl or self.default_acl

        paths_to_make = []
        for segment in path[1:].split("/"):
            if not paths_to_make:
                paths_to_make.append("/" + segment)
                continue

            paths_to_make.append("/".join([paths_to_make[-1], segment]))

        while paths_to_make:
            path = paths_to_make[0]

            if self.features.create_with_stat:
                request = protocol.Create2Request(path=path, acl=acl)
            else:
                request = protocol.CreateRequest(path=path, acl=acl)
            request.set_flags(
                ephemeral=False, sequential=False,
                container=self.features.containers
            )

            try:
                yield self.send(request)
            except exc.NodeExists:
                pass

            paths_to_make.pop(0)

    @gen.coroutine
    def delete(self, path, force=False):
        path = self.normalize_path(path)

        if not force and path in self.stat_cache:
            version = self.stat_cache[path].version
        else:
            version = -1

        yield self.send(protocol.DeleteRequest(path=path, version=version))

    @gen.coroutine
    def get_data(self, path, watch=False):
        path = self.normalize_path(path)

        response = yield self.send(
            protocol.GetDataRequest(path=path, watch=watch)
        )
        raise gen.Return(response.data)

    @gen.coroutine
    def set_data(self, path, data, force=False):
        path = self.normalize_path(path)

        if not force and path in self.stat_cache:
            version = self.stat_cache[path].version
        else:
            version = -1

        yield self.send(
            protocol.SetDataRequest(path=path, data=data, version=version)
        )

    @gen.coroutine
    def get_children(self, path, watch=False):
        path = self.normalize_path(path)

        response = yield self.send(
            protocol.GetChildren2Request(path=path, watch=watch)
        )
        raise gen.Return(response.children)

    @gen.coroutine
    def get_acl(self, path):
        path = self.normalize_path(path)

        response = yield self.send(protocol.GetACLRequest(path=path))
        raise gen.Return(response.acl)

    @gen.coroutine
    def set_acl(self, path, acl, force=False):
        path = self.normalize_path(path)

        if not force and path in self.stat_cache:
            version = self.stat_cache[path].version
        else:
            version = -1

        yield self.send(
            protocol.SetACLRequest(path=path, acl=acl, version=version)
        )

    def begin_transaction(self):
        return Transaction(self)
