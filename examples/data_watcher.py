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


def watcher_callback(new_data):
    left, right = new_data.split(":")
    log.info("Left: %s, Right: %s", left, right)


@gen.coroutine
def run(client, args):
    yield client.start()

    try:
        yield client.create(args.path)
    except exc.NodeExists:
        pass

    watcher = client.recipes.DataWatcher()

    watcher.add_callback(args.path, watcher_callback)

    choices = ["foo:bar", "bwee:bwoo", "derp:hork"]

    for _ in range(5):
        yield client.set_data(args.path, data=random.choice(choices))
        yield gen.sleep(1)
