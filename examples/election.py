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

    order = list(range(args.workers))
    random.shuffle(order)

    yield [worker(i, client, args) for i in order]

    yield client.close()


@gen.coroutine
def worker(number, client, args):
    election = client.recipes.LeaderElection(args.znode_path)

    yield election.join()

    if election.has_leadership:
        log.info("[WORKER #%d] I am the leader!", number)
    else:
        log.info("[WORKER #%d] not the leader.", number)

    yield election.resign()
