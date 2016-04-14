import logging
import random

from tornado import gen


log = logging.getLogger()


def arguments(parser):
    parser.add_argument(
        "--path", "-b", type=str, default="/example-barrier",
        help="ZNode path to use for the double barrier."
    )
    parser.add_argument(
        "--workers", "-w", type=int, default=5,
        help="Number of worker coroutines."
    )
    parser.add_argument(
        "--min-workers", "-m", type=int, default=3,
        help="Minimum number of workers required to lift the barrier."
    )


@gen.coroutine
def run(client, args):
    yield client.start()

    workers = []

    for i in range(args.workers):
        workers.append(worker(i, client, args))
        yield gen.sleep(1)

    yield workers

    yield client.close()


@gen.coroutine
def worker(number, client, args):
    log.info("[WORKER #%d] Starting up", number)

    barrier = client.recipes.DoubleBarrier(args.path, args.min_workers)

    log.info("[WORKER #%d] Entering barrier...", number)
    yield barrier.enter()

    workload = random.choice([3, 4, 5])

    log.info("[WORKER #%d] My workload is %d", number, workload)

    for i in range(workload):
        log.info("[WORKER #%d] Doing task %d", number, i + 1)
        yield gen.sleep(1)

    log.info("[WORKER #%d] Workload complete, leaving barrier", number)
    yield barrier.leave()

    log.info("[WORKER #%d] All done!", number)
