"""The codec, against the worked examples in FORMAT.md.

Those examples are the contract every port must satisfy byte-for-byte, so
they are the right thing to assert against — a round-trip test alone
would pass happily on a private format.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone

import pytest

from bjmsg.binjson import (
    BinjsonError, ObjectId, Pointer, decode, decode_stream, encode, value_size,
)


@pytest.mark.parametrize("value,expected", [
    (42, "032a00000000000000"),
    ("hi", "05020000006869"),
    ([1, True], "100e0000000200000003010000000000000002"),
    ({"a": 1}, "1112000000010000000100000061030100000000000000"),
    (None, "00"),
    (True, "02"),
    (False, "01"),
])
def test_format_md_worked_examples(value, expected):
    assert encode(value).hex() == expected
    assert decode(bytes.fromhex(expected)) == value


@pytest.mark.parametrize("value", [
    None, True, False, 0, 1, -1, 42, -123456789,
    3.14, -0.5, 1e308, float("inf"),
    "", "hello", "héllo ✓ 日本", "a" * 100000,
    [], [1, 2, 3], [[1], [2, [3]]],
    {}, {"a": 1, "b": "two", "c": [3]},
    {"nested": {"deep": {"deeper": [1, None, True, "x"]}}},
    [1, "two", None, True, False, {"k": "v"}, [1.5]],
    b"", b"\x00\x01\xff",
    2 ** 53 - 1, -(2 ** 53 - 1),
])
def test_round_trip(value):
    assert decode(encode(value)) == value


def test_nan_round_trips_as_nan():
    assert math.isnan(decode(encode(float("nan"))))


def test_bool_is_not_an_int():
    # bool subclasses int in Python; True must not encode as 1.
    assert encode(True) == b"\x02"
    assert decode(encode(True)) is True
    assert decode(encode(1)) == 1 and decode(encode(1)) is not True


def test_bytes_round_trip_as_binary():
    assert decode(encode(b"\x05\x01\x00\x00\x00x")) == b"\x05\x01\x00\x00\x00x"
    assert isinstance(decode(encode(bytearray(b"ab"))), bytes)


def test_datetime_is_an_absolute_instant():
    when = datetime(2026, 8, 7, 12, 30, 15, tzinfo=timezone.utc)
    assert decode(encode(when)) == when
    # A naive datetime is read as UTC rather than as local time, so the
    # same value encodes identically on every host.
    assert decode(encode(datetime(2026, 8, 7, 12, 30, 15))) == when


def test_object_id():
    oid = ObjectId("507f1f77bcf86cd799439011")
    assert encode(oid).hex() == "06507f1f77bcf86cd799439011"
    assert decode(encode(oid)) == oid
    assert str(ObjectId()) != str(ObjectId())


def test_pointer():
    assert decode(encode(Pointer(1234))) == Pointer(1234)


def test_integers_beyond_a_double_become_floats():
    # Past the safe range the JavaScript decoder refuses an INT, so the
    # producers agree to emit FLOAT instead. Precision is lost, which is
    # the point of the boundary.
    big = 2 ** 60
    assert encode(big)[0] == 0x04
    assert decode(encode(big)) == float(big)


def test_value_size_walks_without_decoding():
    blob = encode("hello") + encode([1, 2]) + encode({"a": 1})
    offset = 0
    for expected in ("hello", [1, 2], {"a": 1}):
        size = value_size(blob, offset)
        assert decode(blob[offset:offset + size]) == expected
        offset += size
    assert offset == len(blob)
    assert list(decode_stream(blob)) == ["hello", [1, 2], {"a": 1}]


@pytest.mark.parametrize("bad,message", [
    (b"", "unexpected end"),
    (b"\x0a", "unknown type byte"),
    (b"\x12", "unknown type byte"),
    (b"\x03\x01", "unexpected end"),
    (b"\x05\xff\xff\xff\xff", "unexpected end"),
    (b"\x05\x02\x00\x00\x00a", "unexpected end"),
])
def test_malformed_input_is_rejected(bad, message):
    with pytest.raises(BinjsonError, match=message):
        decode(bad)


def test_trailing_bytes_are_an_error():
    with pytest.raises(BinjsonError, match="trailing"):
        decode(encode(1) + b"\x00")


def test_unsupported_types_are_refused():
    with pytest.raises(BinjsonError, match="cannot encode"):
        encode({1, 2})
    with pytest.raises(BinjsonError, match="keys must be strings"):
        encode({1: "a"})
