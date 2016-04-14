import logging
import random

from tornado import gen

log = logging.getLogger()


def arguments(parser):
    parser.add_argument(
        "--reader-count", "-r", type=int, default=6,
        help="Number of readers to launch.",
    )
    parser.add_argument(
        "--writer-count", "-w", type=int, default=2,
        help="Number of writers to launch.",
    )


@gen.coroutine
def run(client, args):
    workers = ([reader] * args.reader_count) + ([writer] * args.writer_count)
    random.shuffle(workers)

    yield client.start()

    yield [
        worker_func(i, client)
        for i, worker_func in enumerate(workers)
    ]

    yield client.close()


@gen.coroutine
def reader(number, client):
    lock = client.recipes.SharedLock("/examplelock")

    wait = random.choice([1, 2, 3])

    log.info("[READER #%d] Acquiring lock...", number)
    with (yield lock.acquire_read()):
        log.info("[READER #%d] Got lock! Sleeping %d seconds", number, wait)
        yield gen.sleep(wait)
        log.info("[READER #%d] Done!", number)


@gen.coroutine
def writer(number, client):
    lock = client.recipes.SharedLock("/examplelock")

    wait = random.choice([2, 3])

    log.info("[WRITER #%d] Acquiring lock...", number)
    with (yield lock.acquire_write()):
        log.info("[WRITER #%d] Got lock! Sleeping %d seconds", number, wait)
        yield gen.sleep(wait)
        log.info("[WRITER #%d] Done!", number)
