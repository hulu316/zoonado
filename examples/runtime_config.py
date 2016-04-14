import logging

from tornado import ioloop, gen
from zoonado import exc


log = logging.getLogger()


def arguments(parser):
    pass


@gen.coroutine
def run(client, args):
    config_path = "/exampleconfig"
    loop = ioloop.IOLoop.current()

    yield client.start()

    config = client.recipes.TreeCache(config_path)

    yield config.start()

    try:
        yield client.create(config_path + "/running", data="yes")
    except exc.NodeExists:
        yield client.set_data(config_path + "/running", data="yes")

    for path in ["foo", "bar", "bazz", "bloo"]:
        try:
            yield client.create(config_path + "/" + path, data="1")
        except exc.NodeExists:
            yield client.set_data(config_path + "/" + path, data="1")

    loop.add_callback(foo, config)
    loop.add_callback(bar, config)
    loop.add_callback(bazz, config)
    loop.add_callback(bloo, config)

    yield gen.sleep(1)

    yield client.set_data(config_path + "/foo", "3")

    yield gen.sleep(1)

    yield client.set_data(config_path + "/bar", "2")

    yield client.set_data(config_path + "/bazz", "5")

    yield gen.sleep(6)

    yield client.set_data(config_path + "/running", data="no")

    yield gen.sleep(2)

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
