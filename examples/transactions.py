import logging
import random
import threading

from tornado import gen, ioloop
from zoonado import Zoonado


log = logging.getLogger()

monitor_ioloop = None


def arguments(parser):
    pass


@gen.coroutine
def run(client, args):
    yield client.start()

    yield client.create("/shared-znode", ephemeral=True)

    monitor_thread = threading.Thread(target=monitor_data, args=(args,))
    monitor_thread.start()

    threads = [
        threading.Thread(name="A", target=launch_loop, args=(args,)),
        threading.Thread(name="B", target=launch_loop, args=(args,)),
        threading.Thread(name="C", target=launch_loop, args=(args,)),
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    monitor_ioloop.stop()

    monitor_thread.join()

    yield client.close()


def monitor_data(args):
    global monitor_ioloop

    name = threading.current_thread().name
    log.info("Launching loop in thread %s", name)

    io_loop = ioloop.IOLoop()

    io_loop.make_current()

    monitor_ioloop = io_loop

    @gen.coroutine
    def monitor():
        client = Zoonado(args.servers, chroot=args.chroot)
        yield client.start()

        def data_callback(new_data):
            log.info("Shared data set to '%s'", new_data)

        watcher = client.recipes.DataWatcher()
        watcher.add_callback("/shared-znode", data_callback)

        yield gen.moment

    io_loop.add_callback(monitor)

    io_loop.start()


def launch_loop(args):
    name = threading.current_thread().name
    log.info("Launching loop in thread %s", name)

    io_loop = ioloop.IOLoop()

    io_loop.make_current()

    io_loop.add_callback(update_loop, name, args, io_loop)

    io_loop.start()


@gen.coroutine
def update_loop(name, args, io_loop):
    log.info("[LOOP %s] starting up!", name)

    client = Zoonado(args.servers, chroot=args.chroot)

    try:
        yield client.start()

        for i in range(5):
            yield client.exists("/shared-znode")
            expected_version = client.stat_cache["/shared-znode"].version

            yield gen.sleep(random.choice([.2, .4, .5]))

            log.info(
                "[LOOP %s] I expect the shared znode to have version %s",
                name, client.stat_cache["/shared-znode"].version
            )

            txn = client.begin_transaction()

            txn.create("/znode-" + name, ephemeral=True)
            txn.check_version("/shared-znode", expected_version)
            txn.set_data("/shared-znode", "altered by loop %s!" % name)
            txn.delete("/znode-" + name)

            log.info("[LOOP %s] committing...", name)
            result = yield txn.commit()
            if not result:
                log.info("[LOOP %s] rolled back!", name)

        yield client.close()
    finally:
        io_loop.stop()
