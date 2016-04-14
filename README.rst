=======================================
Zoonado: Async Tornado Zookeeper Client
=======================================

.. image::
    https://img.shields.io/pypi/v/zoonado.svg
    :target: http://pypi.python.org/pypi/zoonado
    :alt: Python Package Version
.. image::
    https://readthedocs.org/projects/zoonado/badge/?version=latest
    :alt: Documentation Status
    :target: http://zoonado.readthedocs.org/en/latest/
.. image::
    https://travis-ci.org/wglass/zoonado.svg?branch=master
    :alt: Build Status
    :target: https://travis-ci.org/wglass/zoonado
.. image::
    https://landscape.io/github/wglass/zoonado/master/landscape.svg?style=flat
    :alt: Code Health
    :target: https://landscape.io/github/wglass/zoonado/master
.. image::
    https://codecov.io/github/wglass/zoonado/coverage.svg?branch=master
    :alt: Codecov.io
    :target: https://codecov.io/github/wglass/zoonado?branch=master

..

  Zoonado is a Zookeeper_ python client using Tornado_ to achieve async I/O.


.. contents:: :local:


Installation
~~~~~~~~~~~~

Zoonado is available via PyPI_, installation is as easy as::

  pip install zoonado


Quick Example
~~~~~~~~~~~~~

::

   from tornado import gen
   from zoonado import Zoonado

   @gen.coroutine
   def run():
       zk = Zoonado("zk01,zk02,zk03", chroot="/shared/namespace")

       yield zk.start()

       yield zk.create("/foo/bar", data="bazz", ephemeral=True)

       yield zk.set_data("/foo/bar", "bwee")

       yield zk.close()


Development
~~~~~~~~~~~

The code is hosted on GitHub_


To file a bug or possible enhancement see the `Issue Tracker`_, also found
on GitHub.


License
~~~~~~~

Lighthouse is licensed under the terms of the Apache license (2.0).  See the
LICENSE_ file for more details.


.. _Zookeeper: https://zookeeper.apache.org
.. _Tornado: http://tornadoweb.org
.. _PyPI: https://pypi.python.org/pypi/zoonado
.. _GitHub: https://github.com/wglass/zoonado
.. _`Issue Tracker`: https://github.com/wglass/zoonado/issues
.. _LICENSE: https://github.com/wglass/zoonado/blob/master/LICENSE
