"""binjson — the compact binary encoding sukkal carries.

A port of the format specified in third_party/binjson/FORMAT.md, which is
a contract rather than a description: every multi-byte scalar is
little-endian, every length is a uint32, and all text is UTF-8. Bytes
produced here are the bytes the C broker and the JavaScript client
produce.

One deliberate difference from the JavaScript producer, which has only
one numeric type: there, `1.0` is an integer and encodes as INT. Here
`int` encodes as INT and `float` as FLOAT, so a Python value round-trips
to itself. Both are valid encodings of the same number and every decoder
accepts either.
"""
from __future__ import annotations

import struct
from datetime import datetime, timezone
from typing import Any, BinaryIO, Union

__all__ = [
    "encode", "decode", "value_size", "ObjectId", "Pointer",
    "BinjsonError", "TYPE",
]


class TYPE:
    NULL = 0x00
    FALSE = 0x01
    TRUE = 0x02
    INT = 0x03
    FLOAT = 0x04
    STRING = 0x05
    OID = 0x06
    DATE = 0x07
    POINTER = 0x08
    BINARY = 0x09
    ARRAY = 0x10
    OBJECT = 0x11


class BinjsonError(ValueError):
    """Malformed input, or a value that has no encoding."""


#: What the JavaScript producer will encode as INT rather than FLOAT.
#: Staying inside it keeps a number decodable by a JavaScript client,
#: which narrows an int64 to a double and refuses to lose precision.
MAX_SAFE_INT = 2 ** 53 - 1
MIN_SAFE_INT = -MAX_SAFE_INT

_U32 = struct.Struct("<I")
_I64 = struct.Struct("<q")
_U64 = struct.Struct("<Q")
_F64 = struct.Struct("<d")


class ObjectId:
    """A 12-byte MongoDB-compatible identifier."""

    __slots__ = ("_bytes",)

    def __init__(self, value: Union[str, bytes, "ObjectId", None] = None):
        if value is None:
            import os
            import time
            self._bytes = (int(time.time()).to_bytes(4, "big") + os.urandom(8))
        elif isinstance(value, ObjectId):
            self._bytes = value._bytes
        elif isinstance(value, (bytes, bytearray)):
            if len(value) != 12:
                raise BinjsonError("an ObjectId is exactly 12 bytes")
            self._bytes = bytes(value)
        elif isinstance(value, str):
            if len(value) != 24:
                raise BinjsonError("an ObjectId is 24 hex characters")
            try:
                self._bytes = bytes.fromhex(value)
            except ValueError as exc:
                raise BinjsonError("an ObjectId is 24 hex characters") from exc
        else:
            raise BinjsonError(f"cannot make an ObjectId from {type(value).__name__}")

    def to_bytes(self) -> bytes:
        return self._bytes

    @property
    def timestamp(self) -> datetime:
        secs = int.from_bytes(self._bytes[:4], "big")
        return datetime.fromtimestamp(secs, tz=timezone.utc)

    def __str__(self) -> str:
        return self._bytes.hex()

    def __repr__(self) -> str:
        return f"ObjectId('{self}')"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ObjectId) and other._bytes == self._bytes

    def __hash__(self) -> int:
        return hash(self._bytes)


class Pointer:
    """A file offset, for binjson's own on-disk structures."""

    __slots__ = ("offset",)

    def __init__(self, offset: int):
        if offset < 0 or offset > 2 ** 64 - 1:
            raise BinjsonError("a pointer offset is an unsigned 64-bit integer")
        self.offset = offset

    def __repr__(self) -> str:
        return f"Pointer({self.offset})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Pointer) and other.offset == self.offset

    def __hash__(self) -> int:
        return hash(self.offset)


# ---- encoding ------------------------------------------------------------

def encode(value: Any) -> bytes:
    """Encode one value. Returns the complete encoding, type byte first."""
    out = bytearray()
    _encode_into(value, out)
    return bytes(out)


def _encode_into(value: Any, out: bytearray) -> None:
    # bool before int: in Python bool IS an int, and True would otherwise
    # encode as the integer 1.
    if value is None:
        out.append(TYPE.NULL)
    elif value is True:
        out.append(TYPE.TRUE)
    elif value is False:
        out.append(TYPE.FALSE)
    elif isinstance(value, ObjectId):
        out.append(TYPE.OID)
        out += value.to_bytes()
    elif isinstance(value, Pointer):
        out.append(TYPE.POINTER)
        out += _U64.pack(value.offset)
    elif isinstance(value, datetime):
        out.append(TYPE.DATE)
        # Naive datetimes are taken as UTC rather than local time: the
        # wire format is an absolute instant, and guessing a timezone
        # from the machine would make the same value encode differently
        # on two hosts.
        when = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        out += _I64.pack(int(when.timestamp() * 1000))
    elif isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        out.append(TYPE.BINARY)
        out += _U32.pack(len(raw))
        out += raw
    elif isinstance(value, int):
        if not (MIN_SAFE_INT <= value <= MAX_SAFE_INT):
            # Beyond a double's exact range. The JavaScript producer
            # switches to FLOAT here and its decoder refuses an INT this
            # large, so matching it keeps the value readable by every
            # client rather than only by this one.
            out.append(TYPE.FLOAT)
            out += _F64.pack(float(value))
        else:
            out.append(TYPE.INT)
            out += _I64.pack(value)
    elif isinstance(value, float):
        out.append(TYPE.FLOAT)
        out += _F64.pack(value)
    elif isinstance(value, str):
        raw = value.encode("utf-8")
        out.append(TYPE.STRING)
        out += _U32.pack(len(raw))
        out += raw
    elif isinstance(value, (list, tuple)):
        _encode_container(TYPE.ARRAY, len(value), value, out, keyed=False)
    elif isinstance(value, dict):
        _encode_container(TYPE.OBJECT, len(value), value.items(), out, keyed=True)
    else:
        raise BinjsonError(f"cannot encode {type(value).__name__}")


def _encode_container(type_byte, count, items, out, keyed):
    out.append(type_byte)
    size_at = len(out)
    out += b"\0\0\0\0"          # content size, filled in once it is known
    content_at = len(out)
    out += _U32.pack(count)

    for item in items:
        if keyed:
            key, item = item
            if not isinstance(key, str):
                raise BinjsonError("object keys must be strings")
            raw = key.encode("utf-8")
            # Keys carry no type byte — they are bare length-prefixed
            # UTF-8, which is the one place the format is not uniform.
            out += _U32.pack(len(raw))
            out += raw
        _encode_into(item, out)

    _U32.pack_into(out, size_at, len(out) - content_at)


# ---- decoding ------------------------------------------------------------

def decode(data: Union[bytes, bytearray, memoryview]) -> Any:
    """Decode one value. Trailing bytes are an error, not a stream."""
    view = memoryview(data)
    value, offset = _decode_at(view, 0)
    if offset != len(view):
        raise BinjsonError(
            f"{len(view) - offset} trailing byte(s) after the value"
        )
    return value


def decode_stream(data: Union[bytes, bytearray, memoryview]):
    """Yield every value in a sequence of concatenated encodings."""
    view = memoryview(data)
    offset = 0
    while offset < len(view):
        value, offset = _decode_at(view, offset)
        yield value


def _need(view, offset, n, what):
    if offset + n > len(view):
        raise BinjsonError(f"unexpected end of data reading {what}")


def _u32(view, offset):
    _need(view, offset, 4, "a length")
    return _U32.unpack_from(view, offset)[0], offset + 4


def _decode_at(view, offset):
    _need(view, offset, 1, "a type byte")
    kind = view[offset]
    offset += 1

    if kind == TYPE.NULL:
        return None, offset
    if kind == TYPE.TRUE:
        return True, offset
    if kind == TYPE.FALSE:
        return False, offset
    if kind == TYPE.INT:
        _need(view, offset, 8, "an int")
        return _I64.unpack_from(view, offset)[0], offset + 8
    if kind == TYPE.FLOAT:
        _need(view, offset, 8, "a float")
        return _F64.unpack_from(view, offset)[0], offset + 8
    if kind == TYPE.DATE:
        _need(view, offset, 8, "a date")
        ms = _I64.unpack_from(view, offset)[0]
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc), offset + 8
    if kind == TYPE.POINTER:
        _need(view, offset, 8, "a pointer")
        return Pointer(_U64.unpack_from(view, offset)[0]), offset + 8
    if kind == TYPE.OID:
        _need(view, offset, 12, "an ObjectId")
        return ObjectId(bytes(view[offset:offset + 12])), offset + 12
    if kind == TYPE.STRING:
        length, offset = _u32(view, offset)
        _need(view, offset, length, "a string")
        return bytes(view[offset:offset + length]).decode("utf-8"), offset + length
    if kind == TYPE.BINARY:
        length, offset = _u32(view, offset)
        _need(view, offset, length, "a binary blob")
        return bytes(view[offset:offset + length]), offset + length

    if kind in (TYPE.ARRAY, TYPE.OBJECT):
        size, offset = _u32(view, offset)
        _need(view, offset, size, "container content")
        end = offset + size
        count, offset = _u32(view, offset)

        if kind == TYPE.ARRAY:
            items = []
            for _ in range(count):
                item, offset = _decode_at(view, offset)
                items.append(item)
            result: Any = items
        else:
            obj = {}
            for _ in range(count):
                klen, offset = _u32(view, offset)
                _need(view, offset, klen, "an object key")
                key = bytes(view[offset:offset + klen]).decode("utf-8")
                offset += klen
                obj[key], offset = _decode_at(view, offset)
            result = obj

        if offset != end:
            raise BinjsonError("container content size does not match its elements")
        return result, offset

    raise BinjsonError(f"unknown type byte: 0x{kind:02x}")


def value_size(data: Union[bytes, bytearray, memoryview], offset: int = 0) -> int:
    """
    Total on-wire size of the value at `offset`, without decoding it —
    which is how a stream of concatenated values is walked.
    """
    view = memoryview(data)
    _need(view, offset, 1, "a type byte")
    kind = view[offset]
    if kind in (TYPE.NULL, TYPE.TRUE, TYPE.FALSE):
        return 1
    if kind in (TYPE.INT, TYPE.FLOAT, TYPE.DATE, TYPE.POINTER):
        return 9
    if kind == TYPE.OID:
        return 13
    if kind in (TYPE.STRING, TYPE.BINARY, TYPE.ARRAY, TYPE.OBJECT):
        length, _ = _u32(view, offset + 1)
        return 1 + 4 + length
    raise BinjsonError(f"unknown type byte: 0x{kind:02x}")
