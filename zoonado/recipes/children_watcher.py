from __future__ import unicode_literals

import logging

from tornado import gen
from zoonado import WatchEvent

from .base_watcher import BaseWatcher


log = logging.getLogger(__name__)


class ChildrenWatcher(BaseWatcher):

    watched_event = WatchEvent.CHILDREN_CHANGED

    @gen.coroutine
    def fetch(self, path):
        log.debug("Fetching children for %s", path)
        children = yield self.client.get_children(path=path, watch=True)
        raise gen.Return(children)
