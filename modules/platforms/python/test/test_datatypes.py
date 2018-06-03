import pytest

from datatypes import BaseDataType
from datatypes.type_codes import *


class MockSocket:

    def __init__(self, buffer: bytes):
        self.buffer = buffer
        self.pos = 0

    def send(self, data: bytes):
        print(f'Received: {data}')

    def recv(self, buffersize: int):
        received = self.buffer[self.pos:self.pos+buffersize]
        self.pos += buffersize
        return received


@pytest.mark.parametrize(
    'conn, expected_value',
    [
        (MockSocket(b'\x01\xfe'), -2),
        (MockSocket(b'\x01\x02'), 2),
    ]
)
def test_byte(conn, expected_value):
    byte_var = BaseDataType(conn)
    assert byte_var.type_code == TC_BYTE
    assert byte_var.value == expected_value
