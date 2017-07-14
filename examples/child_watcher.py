import logging
import random
from tornado import gen
from zoonado import exc

log = logging.getLogger()


def arguments(parser):
    parser.add_argument(
        "--path", "-p", type=str, default="/examplewatcher",
        help="ZNode path to use for the example."
    )


def watcher_callback(children):
    children.sort()
    log.info("There are %d items now: %s", len(children), ", ".join(children))


@gen.coroutine
def run(client, args):
    yield client.start()

    try:
        yield client.create(args.path)
    except exc.NodeExists:
        pass

    watcher = client.recipes.ChildrenWatcher()

    watcher.add_callback(args.path, watcher_callback)

    to_make = ["cat", "dog", "mouse", "human"]
    random.shuffle(to_make)

    for item in to_make:
        yield client.create(args.path + "/" + item, ephemeral=True)
        yield gen.sleep(1)

    for item in to_make:
        yield client.delete(args.path + "/" + item)
        yield gen.sleep(1)
