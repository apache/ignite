#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import ctypes
from random import randint

import attr

from pygridgain.api.result import APIResult
from pygridgain.constants import *
from pygridgain.queries import response


def get_response_class(obj: object, sql: bool = False):
    """
    Response class factory.

    :param obj: cache, connection or client object,
    :param sql: (optional) return normal (default) or SQL response class,
    :return: response class.
    """
    template = 'SQLResponse{}{}{}' if sql else 'Response{}{}{}'
    return getattr(response, template.format(*obj.get_protocol_version()))


@attr.s
class Query:
    op_code = attr.ib(type=int)
    following = attr.ib(type=list, factory=list)
    query_id = attr.ib(type=int, default=None)
    _query_c_type = None

    @classmethod
    def build_c_type(cls):
        if cls._query_c_type is None:
            cls._query_c_type = type(
                cls.__name__,
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': [
                        ('length', ctypes.c_int),
                        ('op_code', ctypes.c_short),
                        ('query_id', ctypes.c_longlong),
                    ],
                },
            )
        return cls._query_c_type

    def from_python(self, values: dict = None):
        if values is None:
            values = {}
        buffer = b''

        header_class = self.build_c_type()
        header = header_class()
        header.op_code = self.op_code
        if self.query_id is None:
            header.query_id = randint(MIN_LONG, MAX_LONG)

        for name, c_type in self.following:
            buffer += c_type.from_python(values[name])

        header.length = (
            len(buffer)
            + ctypes.sizeof(header_class)
            - ctypes.sizeof(ctypes.c_int)
        )
        return header.query_id, bytes(header) + buffer

    def perform(
        self, conn: 'Connection', query_params: dict = None,
        response_config: list = None, sql: bool = False, **kwargs,
    ) -> APIResult:
        """
        Perform query and process result.

        :param conn: connection to GridGain server,
        :param query_params: (optional) dict of named query parameters.
         Defaults to no parameters,
        :param response_config: (optional) response configuration − list of
         (name, type_hint) tuples. Defaults to empty return value,
        :param sql: (optional) use normal (default) or SQL response class,
        :return: instance of :class:`~pygridgain.api.result.APIResult` with raw
         value (may undergo further processing in API functions).
        """
        _, send_buffer = self.from_python(query_params)
        conn.send(send_buffer)
        response_class = get_response_class(conn, sql)
        response_struct = response_class(response_config, **kwargs)
        response_ctype, recv_buffer = response_struct.parse(conn)
        response = response_ctype.from_buffer_copy(recv_buffer)

        # this test depends on protocol version
        if getattr(response, 'flags', False) & RHF_TOPOLOGY_CHANGED:
            # update latest affinity version
            conn.client.affinity_version = (
                response.affinity_version, response.affinity_minor
            )

        # build result
        result = APIResult(response)
        if result.status == 0:
            result.value = response_struct.to_python(response)
        return result


class ConfigQuery(Query):
    """
    This is a special query, used for creating caches with configuration.
    """
    _query_c_type = None

    @classmethod
    def build_c_type(cls):
        if cls._query_c_type is None:
            cls._query_c_type = type(
                cls.__name__,
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': [
                        ('length', ctypes.c_int),
                        ('op_code', ctypes.c_short),
                        ('query_id', ctypes.c_longlong),
                        ('config_length', ctypes.c_int),
                    ],
                },
            )
        return cls._query_c_type

    def from_python(self, values: dict = None):
        if values is None:
            values = {}
        buffer = b''

        header_class = self.build_c_type()
        header = header_class()
        header.op_code = self.op_code
        if self.query_id is None:
            header.query_id = randint(MIN_LONG, MAX_LONG)

        for name, c_type in self.following:
            buffer += c_type.from_python(values[name])

        header.length = (
            len(buffer)
            + ctypes.sizeof(header_class)
            - ctypes.sizeof(ctypes.c_int)
        )
        header.config_length = header.length - ctypes.sizeof(header_class)
        return header.query_id, bytes(header) + buffer
