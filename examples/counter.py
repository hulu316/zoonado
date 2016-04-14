import logging
import random

from tornado import gen
from zoonado import exc


log = logging.getLogger(__name__)


def arguments(parser):
    parser.add_argument(
        "--path", "-b", type=str, default="/example-counter",
        help="ZNode path to use for the barrier."
    )
    parser.add_argument(
        "--workers", "-w", type=int, default=5,
        help="Number of worker coroutines."
    )


@gen.coroutine
def run(client, args):
    yield client.start()

    value_history = []

    def callback(new_value):
        value_history.append(new_value)

    watcher = client.recipes.DataWatcher()
    watcher.add_callback(args.path, callback)

    try:
        data = yield client.get_data(args.path)
        log.info("Initial value is %s", data)
    except exc.NoNode:
        log.info("Initial value is blank")

    yield [
        worker(number, client, args)
        for number in range(args.workers)
    ]

    log.info("Value history: %s", " -> ".join(value_history))

    yield client.close()


@gen.coroutine
def worker(number, client, args):
    log.info("[WORKER #%d] Starting up", number)

    counter = client.recipes.Counter(args.path)

    yield counter.start()

    for i in range(2):
        op = random.choice(["incr", "incr", "decr", "decr", "set"])
        if op == "incr":
            log.info("[WORKER #%d] Incrementing count", number)
            yield counter.incr()
        elif op == "decr":
            log.info("[WORKER #%d] Decrementing count", number)
            yield counter.decr()
        else:
            new_value = random.choice([4, 8, 12, 16])
            log.info("[WORKER #%d] Setting count to %d", number, new_value)
            yield counter.set_value(new_value)

    yield counter.stop()
