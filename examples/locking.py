import logging
import random

from tornado import gen


log = logging.getLogger()


def arguments(parser):
    parser.add_argument(
        "--workers", "-w", type=int, default=3,
        help="Number of workers to launch."
    )
    parser.add_argument(
        "--lock-path", "-p", type=str, default="examplelock",
        help="ZNode path to use for the lock."
    )


@gen.coroutine
def run(client, args):
    log.info("Launching %d workers.", args.workers)

    yield client.start()

    order = list(range(args.workers))
    random.shuffle(order)

    yield [work(i, client, args) for i in order]

    yield client.close()


@gen.coroutine
def work(number, client, args):
    lock = client.recipes.Lock(args.lock_path)

    num_iterations = 3

    log.info("[WORKER #%d] Acquiring lock...", number)
    with (yield lock.acquire()) as check:
        log.info("[WORKER #%d] Got lock!", number)

        for i in range(num_iterations):
            wait = random.choice([1, 2, 3])
            if not check():
                log.warn("[WORKER #%d] lost my lock!", number)
                break

            log.info("[WORKER #%d] working %d secs", number, wait)
            yield gen.sleep(wait)

        log.info("[WORKER #%d] Done!", number)
