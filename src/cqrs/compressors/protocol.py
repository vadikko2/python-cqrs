import typing


class Compressor(typing.Protocol):
    def compress(self, value: bytes) -> bytes:
        """Compress value"""

    def decompress(self, value: bytes) -> bytes:
        """Decompress compressed value"""
