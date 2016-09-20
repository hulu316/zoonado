from setuptools import setup, find_packages

from zoonado import __version__


setup(
    name="zoonado",
    version=__version__,
    description="Async tornado client for Zookeeper.",
    author="William Glass",
    author_email="william.glass@gmail.com",
    url="http://github.com/wglass/zoonado",
    license="Apache",
    keywords=["zookeeper", "tornado", "async", "distributed"],
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=[
        "tornado>=4.1",
        "six"
    ],
    entry_points={
        "zoonado.recipes": [
            "data_watcher = zoonado.recipes.data_watcher:DataWatcher",
            "children_watcher = zoonado.recipes.children_watcher:ChildrenWatcher",
            "lock = zoonado.recipes.lock:Lock",
            "shared_lock = zoonado.recipes.shared_lock:SharedLock",
            "lease = zoonado.recipes.lease:Lease",
            "barrier = zoonado.recipes.barrier:Barrier",
            "double_barrier = zoonado.recipes.double_barrier:DoubleBarrier",
            "election = zoonado.recipes.election:LeaderElection",
            "party = zoonado.recipes.party:Party",
            "counter = zoonado.recipes.counter:Counter",
            "tree_cache = zoonado.recipes.tree_cache:TreeCache",
            "allocator = zoonado.recipes.allocator:Allocator",
        ],
    },
    tests_require=[
        "nose",
        "mock",
        "coverage",
        "flake8>=3.0.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: Implementation",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
    ],
)
