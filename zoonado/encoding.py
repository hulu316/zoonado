import logging

import six


log = logging.getLogger(__name__)


def default_encoder(data):
    if data is None or isinstance(data, six.binary_type):
        return data

    try:
        return data.encode("utf8")
    except Exception:
        log.exception("Error encoding data: %r", data)
        raise


def default_decoder(data):
    if data is None:
        return data

    try:
        return data.decode("utf8")
    except UnicodeDecodeError:
        return data
