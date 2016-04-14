import collections
import itertools
import logging
import random

from tornado import gen, ioloop
from zoonado.recipes.allocator import round_robin


log = logging.getLogger(__name__)


ANIMALS = ["cat", "dog", "mouse", "human"]


def arguments(parser):
    parser.add_argument(
        "znode", type=str,
        help="Path of the base znode to use."
    )
    parser.add_argument(
        "--workers", "-w", type=int, default=5,
        help="Number of worker coroutines to launch."
    )
    parser.add_argument(
        "--items", "-n", type=int, default=17,
        help="Number of items to allocate amongst the workers."
    )
    parser.add_argument(
        "--alloc-func", "-a", default="round_robin",
        choices=allocation_functions.keys(),
        help="Which allocation function to use."
    )


@gen.coroutine
def run(client, args):
    items = set([
        "%s::%s" % (i, random.choice(ANIMALS))
        for i in range(args.items)
    ])
    allocation_function = allocation_functions[args.alloc_func]

    yield client.start()

    for i in range(args.workers):
        ioloop.IOLoop.current().add_callback(
            worker, i, client, args.znode, allocation_function, items
        )

    yield gen.sleep(10)


@gen.coroutine
def worker(number, client, znode_path, allocation_fn, items):
    name = "worker_%s" % number

    allocator = client.recipes.Allocator(znode_path, name, allocation_fn)

    yield allocator.start()

    yield allocator.update(items)

    while True:
        log.info("[WORKER %d] My set: %s", number, allocator.allocation)
        yield gen.sleep(2)


def animal_buckets(members, items):
    animal_assignment = {}
    for member, animal in zip(itertools.cycle(members), ANIMALS):
        animal_assignment[animal] = member

    allocation = collections.defaultdict(set)
    for member, item in zip(itertools.cycle(members), items):
        animal = item.split("::")[1]
        allocation[animal_assignment[animal]].add(item)

    return allocation


allocation_functions = {
    "round_robin": round_robin,
    "buckets": animal_buckets,
}
