from __future__ import unicode_literals

import functools
import logging

from tornado import gen, concurrent

from zoonado import protocol, exc

from .recipes.proxy import RecipeProxy
from .session import Session
from .transaction import Transaction
from .features import Features
from .encoding import default_encoder, default_decoder


log = logging.getLogger(__name__)


def znode_method(fn):

    @functools.wraps(fn)
    def wrapper(client, path, *args, **kwargs):
        path = client.normalize_path(path)

        return fn(client, path, *args, **kwargs)

    return wrapper


class Zoonado(object):

    def __init__(
            self,
            servers,
            chroot=None,
            data_encoder=None,
            data_decoder=None,
            session_timeout=10,
            default_acl=None,
            retry_policy=None,
            allow_read_only=False,
    ):
        self.chroot = None
        if chroot:
            self.chroot = self.normalize_path(chroot)
            log.info("Using chroot '%s'", self.chroot)

        self.data_encoder = data_encoder or default_encoder
        self.data_decoder = data_decoder or default_decoder

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
    @znode_method
    def exists(self, path, watch=False):
        try:
            yield self.send(protocol.ExistsRequest(path=path, watch=watch))
        except exc.NoNode:
            raise gen.Return(False)

        raise gen.Return(True)

    @gen.coroutine
    @znode_method
    def create(
            self, path, data=None, acl=None,
            ephemeral=False, sequential=False, container=False,
    ):
        if container and not self.features.containers:
            raise ValueError("Cannot create container, feature unavailable.")

        acl = acl or self.default_acl

        data = self.data_encoder(data)

        if self.features.create_with_stat:
            request_class = protocol.Create2Request
        else:
            request_class = protocol.CreateRequest

        request = request_class(path=path, data=data, acl=acl)
        request.set_flags(ephemeral, sequential, container)

        response = yield self.send(request)

        raise gen.Return(self.denormalize_path(response.path))

    @gen.coroutine
    @znode_method
    def ensure_path(self, path, acl=None):
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
    @znode_method
    def delete(self, path, force=False):
        version = self.determine_znode_version(path, force)

        yield self.send(protocol.DeleteRequest(path=path, version=version))

    @gen.coroutine
    @znode_method
    def get_data(self, path, watch=False):
        response = yield self.send(
            protocol.GetDataRequest(path=path, watch=watch)
        )
        data = self.data_decoder(response.data)

        raise gen.Return(data)

    @gen.coroutine
    @znode_method
    def set_data(self, path, data, force=False):
        data = self.data_encoder(data)
        version = self.determine_znode_version(path, force)

        yield self.send(
            protocol.SetDataRequest(path=path, data=data, version=version)
        )

    @gen.coroutine
    @znode_method
    def get_children(self, path, watch=False):
        response = yield self.send(
            protocol.GetChildren2Request(path=path, watch=watch)
        )
        raise gen.Return(response.children)

    @gen.coroutine
    @znode_method
    def get_acl(self, path):
        response = yield self.send(protocol.GetACLRequest(path=path))
        raise gen.Return(response.acl)

    @gen.coroutine
    @znode_method
    def set_acl(self, path, acl, force=False):
        version = self.determine_znode_version(path, force)

        yield self.send(
            protocol.SetACLRequest(path=path, acl=acl, version=version)
        )

    def begin_transaction(self):
        return Transaction(self)

    def determine_znode_version(self, path, force):
        if not force and path in self.stat_cache:
            return self.stat_cache[path].version
        else:
            return -1
