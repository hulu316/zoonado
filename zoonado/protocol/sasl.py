from __future__ import unicode_literals

from .request import Request
from .response import Response
from .primitives import Buffer


class SASLRequest(Request):
    """
    """
    opcode = 102

    parts = (
        ("token", Buffer)
    )


class SASLResponse(Response):
    """
    """
    opcode = 102

    parts = (
        ("token", Buffer)
    )
