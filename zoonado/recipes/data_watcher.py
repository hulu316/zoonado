from __future__ import unicode_literals

import logging

from tornado import gen
from zoonado import WatchEvent

from .base_watcher import BaseWatcher


log = logging.getLogger(__name__)


class DataWatcher(BaseWatcher):

    watched_event = WatchEvent.DATA_CHANGED

    @gen.coroutine
    def fetch(self, path):
        log.debug("Fetching data for %s", path)
        data = yield self.client.get_data(path=path, watch=True)
        raise gen.Return(data)
