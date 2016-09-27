import os

from flake8.api import legacy as flake8

try:
    import __pypy__  # noqa
    is_pypy = True
except ImportError:
    is_pypy = False


MAX_COMPLEXITY = 11


def test_style():
    if is_pypy:
        # TODO(wglass): these are incredibly slow in pypy for some reason
        return

    for path in ("zoonado", "tests", "examples"):
        python_files = list(get_python_files(path))
        yield create_style_assert(path, python_files)


def get_python_files(path):
    path = os.path.join(os.path.dirname(__file__), "../", path)
    for root, dirs, files in os.walk(path):
        for filename in files:
            if not filename.endswith(".py"):
                continue
            yield os.path.join(root, filename)


def create_style_assert(path, python_files):

    def test_function():
        assert_conforms_to_style(python_files)

    test_name = "test_style__%s" % path

    test_function.__name__ = test_name
    test_function.description = test_name

    return test_function


def assert_conforms_to_style(python_files):
    checker = flake8.get_style_guide(max_complexity=MAX_COMPLEXITY)

    checker.options.jobs = 1
    checker.options.verbose = True
    report = checker.check_files(python_files)

    warnings = report.get_statistics("W")
    errors = report.get_statistics("E")

    assert not (warnings or errors), "\n" + "\n".join([
        "Warnings:",
        "\n".join(warnings),
        "Errors:",
        "\n".join(errors),
    ])
