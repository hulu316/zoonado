def add_metaclass(metaclass):
    """
    Class decorator for creating a class with a metaclass.

    Adapted from the six project:

    https://pythonhosted.org/six/
    """
    vars_to_skip = ('__dict__', '__weakref__')

    def wrapper(cls):
        copied_dict = {
            key: value
            for key, value in cls.__dict__.items()
            if key not in vars_to_skip
        }
        return metaclass(cls.__name__, cls.__bases__, copied_dict)
    return wrapper
