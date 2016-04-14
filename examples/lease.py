import datetime
import logging
import random

from tornado import gen, ioloop
from zoonado import exc


log = logging.getLogger()


def arguments(parser):
    parser.add_argument(
        "--path", "-p", type=str, default="/example-task-lease"
    )
    parser.add_argument(
        "--limit", "-l", type=int, default=1,
        help="Max number of simultaneous leases."
    )


@gen.coroutine
def run(client, args):
    yield client.start()

    try:
        yield client.create(args.path)
    except exc.NodeExists:
        pass

    # simulate a cron job, same task fired at an interval
    for i in range(8):
        ioloop.IOLoop.current().add_callback(limited_task, i, client, args)
        yield gen.sleep(1)


@gen.coroutine
def limited_task(number, client, args):
    lease = client.recipes.Lease(args.path, args.limit)

    seconds = random.choice([1, 2, 3])

    obtained = yield lease.obtain(duration=datetime.timedelta(seconds=seconds))
    if obtained:
        log.info("[ITERATION #%d] Got lease for %d seconds", number, seconds)
    else:
        log.info("[ITERATION #%d] No lease available, can't work", number)
