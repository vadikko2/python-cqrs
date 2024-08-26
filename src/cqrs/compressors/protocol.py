import typing


class Compressor(typing.Protocol):
    def compress(self, value: bytes) -> bytes:
        """Compress value"""
        raise NotImplementedError

    def decompress(self, value: bytes) -> bytes:
        """Decompress compressed value"""
        raise NotImplementedError
