__all__ = ['executor', 'handlers', 'type_check']


def type_check(var, t):
    if not isinstance(var, t):
        raise TypeError("A {} object is needed, but got a {}.".format(t.__class, type(var)))


