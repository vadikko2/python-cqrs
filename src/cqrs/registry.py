import typing
from collections import abc

_VT = typing.TypeVar("_VT")
_KT = typing.TypeVar("_KT")


class InMemoryRegistry(abc.MutableMapping[_KT, _VT]):
    _registry: typing.Dict[_KT, _VT]

    def __init__(self):
        self._registry = dict()

    def __setitem__(self, __key: _KT, __value: _VT) -> None:
        if __key in self._registry:
            raise KeyError(f"{__key} already exists in registry")
        self._registry[__key] = __value

    def __delitem__(self, __key):
        raise TypeError(f"{self.__class__.__name__} has no delete method")

    def __getitem__(self, __key: _KT) -> _VT:
        return self._registry[__key]

    def __len__(self):
        return len(self._registry.keys())

    def __iter__(self):
        return iter(self._registry.keys())
