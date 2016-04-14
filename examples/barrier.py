import logging

from tornado import gen, ioloop


log = logging.getLogger()


def arguments(parser):
    parser.add_argument(
        "--path", "-b", type=str, default="/example-barrier",
        help="ZNode path to use for the barrier."
    )
    parser.add_argument(
        "--workers", "-w", type=int, default=5,
        help="Number of worker coroutines."
    )


@gen.coroutine
def run(client, args):
    yield client.start()

    barrier = client.recipes.Barrier(args.path)

    yield barrier.create()

    for i in range(args.workers):
        yield gen.sleep(1)
        ioloop.IOLoop.current().add_callback(worker, i, client, args.path)

    yield gen.sleep(2)

    yield barrier.lift()

    yield gen.sleep(3)

    yield client.close()


@gen.coroutine
def worker(number, client, barrier_path):
    log.info("[WORKER #%d] Starting up", number)

    barrier = client.recipes.Barrier(barrier_path)

    log.info("[WORKER #%d] Waiting on barrier...", number)
    yield barrier.wait()

    while True:
        log.info("[WORKER #%d] Doing work!", number)
        yield gen.sleep(.3)
