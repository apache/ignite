# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import ctypes
from io import BytesIO
from typing import Union, Optional

import pyignite
import pyignite.utils as ignite_utils

READ_FORWARD = 0
READ_BACKWARD = 1


class BinaryStreamBase:
    def __init__(self, client, buf=None):
        self.client = client
        self.stream = BytesIO(buf) if buf else BytesIO()
        self._buffer = None

    @property
    def compact_footer(self) -> bool:
        return self.client.compact_footer

    @compact_footer.setter
    def compact_footer(self, value: bool):
        self.client.compact_footer = value

    def read(self, size):
        buf = bytearray(size)
        self.stream.readinto(buf)
        return buf

    def read_ctype(self, ctype_class, position=None, direction=READ_FORWARD):
        ctype_len = ctypes.sizeof(ctype_class)

        if position is not None and position >= 0:
            init_position = position
        else:
            init_position = self.tell()

        if direction == READ_FORWARD:
            start, end = init_position, init_position + ctype_len
        else:
            start, end = init_position - ctype_len, init_position

        with self.getbuffer()[start:end] as buf:
            return ctype_class.from_buffer_copy(buf)

    def write(self, buf):
        self._release_buffer()
        return self.stream.write(buf)

    def tell(self):
        return self.stream.tell()

    def seek(self, *args, **kwargs):
        return self.stream.seek(*args, **kwargs)

    def getbuffer(self):
        if self._buffer:
            return self._buffer

        self._buffer = self.stream.getbuffer()
        return self._buffer

    def getvalue(self):
        return self.stream.getvalue()

    def slice(self, start=-1, offset=0):
        start = start if start >= 0 else self.tell()
        with self.getbuffer()[start:start + offset] as buf:
            return bytes(buf)

    def hashcode(self, start, bytes_len):
        with self.getbuffer()[start:start + bytes_len] as buf:
            return ignite_utils.hashcode(buf)

    def _release_buffer(self):
        if self._buffer:
            self._buffer.release()
            self._buffer = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._release_buffer()
        self.stream.close()


class BinaryStream(BinaryStreamBase):
    """
    Synchronous binary stream.
    """
    def __init__(self, client: 'pyignite.Client', buf: Optional[Union[bytes, bytearray, memoryview]] = None):
        """
        :param client: Client instance, required.
        :param buf: Buffer, optional parameter. If not passed, creates empty BytesIO.
        """
        super().__init__(client, buf)

    def get_dataclass(self, header):
        result = self.client.query_binary_type(header.type_id, header.schema_id)
        if not result:
            raise RuntimeError('Binary type is not registered')
        return result

    def register_binary_type(self, *args, **kwargs):
        self.client.register_binary_type(*args, **kwargs)


class AioBinaryStream(BinaryStreamBase):
    """
    Asyncio binary stream.
    """
    def __init__(self, client: 'pyignite.AioClient', buf: Optional[Union[bytes, bytearray, memoryview]] = None):
        """
        Initialize binary stream around buffers.

        :param client: AioClient instance, required.
        :param buf: Buffer, optional parameter. If not passed, creates empty BytesIO.
        """
        super().__init__(client, buf)

    async def get_dataclass(self, header):
        result = await self.client.query_binary_type(header.type_id, header.schema_id)
        if not result:
            raise RuntimeError('Binary type is not registered')
        return result

    async def register_binary_type(self, *args, **kwargs):
        await self.client.register_binary_type(*args, **kwargs)
