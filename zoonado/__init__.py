
version_info = (0, 8, 1)

__version__ = ".".join((str(point) for point in version_info))

try:
    import tornado  # noqa
    from .client import Zoonado  # noqa
    from .protocol import WatchEvent  # noqa
    from .protocol.acl import ACL  # noqa
    from .retry import RetryPolicy  # noqa
except ImportError:  # pragma: no cover
    pass
