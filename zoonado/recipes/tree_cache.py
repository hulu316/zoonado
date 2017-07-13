from __future__ import unicode_literals

import functools
import logging
import json

import six
from tornado import gen, ioloop, concurrent

from .children_watcher import ChildrenWatcher
from .data_watcher import DataWatcher
from .recipe import Recipe


log = logging.getLogger(__name__)


class TreeCache(Recipe):

    sub_recipes = {
        "data_watcher": DataWatcher,
        "child_watcher": ChildrenWatcher,
    }

    def __init__(
            self,
            base_path,
            defaults=None,
            serializer=None, deserializer=None
    ):
        super(TreeCache, self).__init__(base_path)
        self.defaults = defaults or {}
        self.serializer = serializer or json.dumps
        self.deserializer = deserializer or json.loads

        self.root = None

    @gen.coroutine
    def start(self):
        log.debug("Starting znode tree cache at %s", self.base_path)

        self.root = ZNodeCache(
            self.base_path, self.client,
            self.defaults, self.serializer, self.deserializer,
            self.data_watcher, self.child_watcher,
            is_root=True,
        )

        yield self.ensure_path()

        yield self.root.start()

    def stop(self):
        self.root.stop()

    def __getattr__(self, attribute):
        return getattr(self.root, attribute)

    def get_by_relative_path(self, relative_path):
        if relative_path.startswith("/"):
            relative_path = relative_path[1:]

        znode_cache = self.root
        for layer in relative_path.split("/"):
            znode_cache = getattr(znode_cache, layer)

        return znode_cache

    def as_dict(self):
        return self.root.as_dict()


class ZNodeCache(object):

    def __init__(
            self,
            path, client,
            defaults, serializer, deserializer,
            data_watcher, child_watcher,
            is_root=False,
    ):
        self.path = path

        self.client = client

        self.defaults = defaults

        self.serializer = serializer
        self.deserializer = deserializer

        self.data_watcher = data_watcher
        self.child_watcher = child_watcher

        self.children = {}
        self.data = None

        self.children_synced = None
        self.data_synced = None

        self.is_root = is_root

        self.started = concurrent.Future()

    @property
    def dot_path(self):
        return self.path[1:].replace("/", ".")

    @property
    def value(self):
        return self.data

    def __getattr__(self, name):
        if name not in self.children:
            raise AttributeError

        return self.children[name]

    @gen.coroutine
    def start(self):
        self.children_synced = concurrent.Future()
        self.child_watcher.add_callback(self.path, self.child_callback)

        if not self.is_root:
            self.data_synced = concurrent.Future()
            self.data_watcher.add_callback(self.path, self.data_callback)

        yield self.children_synced
        if not self.is_root:
            yield self.data_synced

        self.children_synced = None
        self.data_synced = None

        yield [child.started for child in self.children.values()]

        log.debug("start() of node %s complete", self.dot_path)

        self.started.set_result(None)

    def stop(self):
        self.data_watcher.remove_callback(self.path, self.data_callback)
        self.child_watcher.remove_callback(self.path, self.child_callback)

    @gen.coroutine
    def set_value(self, new_value):
        new_value = self.serializer(new_value)
        log.debug("Setting %s to %r", self.dot_path, new_value)

        self.data_synced = concurrent.Future()

        yield self.client.set_data(self.path, new_value)

        yield self.data_synced
        self.data_synced = None

    def watch(self, fn):

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        return wrapper

    def child_callback(self, new_children):
        removed_children = set(self.children.keys()) - set(new_children)
        added_children = set(new_children) - set(self.children.keys())

        for removed in removed_children:
            log.debug("Removed child %s", self.dot_path + "." + removed)
            child = self.children.pop(removed)
            child.stop()

        for added in added_children:
            log.debug("added child %s", self.dot_path + "." + added)
            self.add_child_znode_cache(added)
            ioloop.IOLoop.current().add_callback(self.children[added].start)

        if not self.started.done():
            self.children_synced.set_result(None)

    def data_callback(self, data):
        self.data = self.deserializer(data)
        self.data_synced.set_result(None)

    def add_child_znode_cache(self, child_name):
        self.children[child_name] = ZNodeCache(
            self.path + "/" + child_name, self.client,
            self.defaults.get(child_name, {}),
            self.serializer, self.deserializer,
            self.data_watcher, self.child_watcher,
        )

    def as_dict(self):
        if self.children:
            return {
                child_path: child_znode.as_dict()
                for child_path, child_znode in six.iteritems(self.children)
            }

        return self.data
