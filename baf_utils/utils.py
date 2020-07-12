from typing import Iterable, Hashable


def to_tuple(lst):
    return tuple(to_tuple(i) if isinstance(i, Iterable) and not isinstance(i, str) else i for i in lst)


def to_key(item):
    """Converts request input argument in a hashable type in order to use it
    as a key"""
    if isinstance(item, Hashable):
        return item
    elif isinstance(item, Iterable):
        return to_tuple(item)
    else:
        raise RuntimeError('This is a situation that should not occur. But '
                           'Markus warned me that it would, recommending '
                           'putting up an error message. So seeing this just '
                           'proves him right again!')