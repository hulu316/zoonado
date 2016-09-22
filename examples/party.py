import logging
import random

from tornado import gen


log = logging.getLogger()


def arguments(parser):
    parser.add_argument(
        "--workers", "-w", type=int, default=5,
        help="Number of workers to launch."
    )
    parser.add_argument(
        "--znode-path", "-p", type=str, default="examplelock",
        help="ZNode path to use for the election."
    )


@gen.coroutine
def run(client, args):
    log.info("Launching %d workers.", args.workers)
    yield client.start()

    yield [
        worker(i, client, args)
        for i in range(args.workers)
    ]

    yield client.close()


@gen.coroutine
def worker(number, client, args):
    party = client.recipes.Party(args.znode_path, "worker_%d" % number)

    log.info("[WORKER #%d] Joining the party", number)

    yield party.join()

    for _ in range(10):
        log.info("[WORKER #%d] Members I see: %s", number, party.members)
        yield gen.sleep(.5)
        should_leave = random.choice([False, False, True])
        if should_leave:
            log.info("[WORKER #%d] Leaving the party temporarily", number)
            yield party.leave()
            yield gen.sleep(1)
            log.info("[WORKER #%d] Rejoining the party", number)
            yield party.join()

    yield party.leave()
