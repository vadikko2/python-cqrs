import zlib


class ZlibCompressor:
    def compress(self, value: bytes) -> bytes:
        return zlib.compress(value)

    def decompress(self, value: bytes) -> bytes:
        return zlib.decompress(value)
