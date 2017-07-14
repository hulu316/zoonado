import logging

from tornado import ioloop, gen
from zoonado import exc


log = logging.getLogger()


def arguments(_):
    pass


@gen.coroutine
def run(client, args):
    config_path = "/exampleconfig"

    yield client.start()

    config = client.recipes.TreeCache(config_path)

    yield config.start()

    try:
        yield client.create(config_path + "/running")
    except exc.NodeExists:
        pass

    yield config.running.set_value("yes")

    for path in ["foo", "bar", "bazz", "bloo"]:
        try:
            yield client.create(config_path + "/" + path)
        except exc.NodeExists:
            pass

    yield [
        config.foo.set_value(1),
        config.bar.set_value(2),
        config.bazz.set_value(1),
        config.bloo.set_value(3),
    ]

    loop = ioloop.IOLoop.current()

    loop.add_callback(foo, config)
    loop.add_callback(bar, config)
    loop.add_callback(bazz, config)
    loop.add_callback(bloo, config)

    yield gen.sleep(1)

    yield config.foo.set_value(3)

    yield gen.sleep(1)

    yield config.bar.set_value(2)

    yield config.bazz.set_value(5)

    yield gen.sleep(6)

    yield config.running.set_value("no")

    yield gen.sleep(6)

    yield client.close()


@gen.coroutine
def foo(config):
    while config.running.value == "yes":
        log.info("[FOO] doing work for %s seconds!", config.foo.value)
        yield gen.sleep(int(config.foo.value))

    log.info("[FOO] no longer working.")


@gen.coroutine
def bar(config):
    while config.running.value == "yes":
        log.info("[BAR] doing work for %s seconds!", config.bar.value)
        yield gen.sleep(int(config.bar.value))

    log.info("[BAR] no longer working.")


@gen.coroutine
def bazz(config):
    while config.running.value == "yes":
        log.info("[BAZZ] doing work for %s seconds!", config.bazz.value)
        yield gen.sleep(int(config.bazz.value))

    log.info("[BAZZ] no longer working.")


@gen.coroutine
def bloo(config):
    while config.running.value == "yes":
        log.info("[BLOO] doing work for %s seconds!", config.bloo.value)
        yield gen.sleep(int(config.bloo.value))

    log.info("[BLOO] no longer working.")
