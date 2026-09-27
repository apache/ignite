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
import inspect
import logging
import time
from io import SEEK_CUR

import attr

from pyignite.api.result import APIResult
from pyignite.connection import Connection, AioConnection
from pyignite.constants import MAX_LONG, RHF_TOPOLOGY_CHANGED
from pyignite.queries import op_codes
from pyignite.queries.response import Response
from pyignite.stream import AioBinaryStream, BinaryStream, READ_BACKWARD

logger = logging.getLogger('.'.join(__name__.split('.')[:-1]))


def query_perform(query_struct, conn, post_process_fun=None, **kwargs):
    async def _async_internal():
        result = await query_struct.perform_async(conn, **kwargs)
        if post_process_fun:
            return post_process_fun(result)
        return result

    def _internal():
        result = query_struct.perform(conn, **kwargs)
        if post_process_fun:
            return post_process_fun(result)
        return result

    if isinstance(conn, AioConnection):
        return _async_internal()
    return _internal()


_QUERY_COUNTER = 0


def _get_query_id():
    global _QUERY_COUNTER
    if _QUERY_COUNTER >= MAX_LONG:
        return 0
    _QUERY_COUNTER += 1
    return _QUERY_COUNTER


_OP_CODES = {code: name for name, code in inspect.getmembers(op_codes) if name.startswith('OP_')}


def _get_op_code_name(code):
    global _OP_CODES
    return _OP_CODES.get(code)


def _sec_to_millis(secs):
    return int(secs * 1000)


@attr.s
class Query:
    op_code = attr.ib(type=int)
    following = attr.ib(type=list, factory=list)
    query_id = attr.ib(type=int)
    response_type = attr.ib(type=type(Response), default=Response)
    _query_c_type = None
    _start_ts = 0.0

    @query_id.default
    def _set_query_id(self):
        return _get_query_id()

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

    def from_python(self, stream, values: dict = None):
        init_pos, header = stream.tell(), self._build_header(stream)
        values = values if values else None

        for name, c_type in self.following:
            c_type.from_python(stream, values[name])

        self.__write_header(stream, header, init_pos)

    async def from_python_async(self, stream, values: dict = None):
        init_pos, header = stream.tell(), self._build_header(stream)
        values = values if values else None

        for name, c_type in self.following:
            await c_type.from_python_async(stream, values[name])

        self.__write_header(stream, header, init_pos)

    def _build_header(self, stream):
        global _QUERY_COUNTER
        header_class = self.build_c_type()
        header_len = ctypes.sizeof(header_class)
        stream.seek(header_len, SEEK_CUR)

        header = header_class()
        header.op_code = self.op_code
        header.query_id = self.query_id

        return header

    @staticmethod
    def __write_header(stream, header, init_pos):
        header.length = stream.tell() - init_pos - ctypes.sizeof(ctypes.c_int)
        stream.seek(init_pos)
        stream.write(header)

    def perform(
        self, conn: Connection, query_params: dict = None,
        response_config: list = None, **kwargs,
    ) -> APIResult:
        """
        Perform query and process result.

        :param conn: connection to Ignite server,
        :param query_params: (optional) dict of named query parameters.
         Defaults to no parameters,
        :param response_config: (optional) response configuration − list of
         (name, type_hint) tuples. Defaults to empty return value,
        :return: instance of :class:`~pyignite.api.result.APIResult` with raw
         value (may undergo further processing in API functions).
        """
        try:
            self._on_query_started(conn)

            with BinaryStream(conn.client) as stream:
                self.from_python(stream, query_params)
                response_data = conn.request(stream.getvalue())

            response_struct = self.response_type(protocol_context=conn.protocol_context,
                                                 following=response_config, **kwargs)

            with BinaryStream(conn.client, response_data) as stream:
                response_ctype = response_struct.parse(stream)
                response = stream.read_ctype(response_ctype, direction=READ_BACKWARD)

            result = self.__post_process_response(conn, response_struct, response)
            if result.status == 0:
                result.value = response_struct.to_python(response)
            self._on_query_finished(conn, result=result)
            return result
        except Exception as e:
            self._on_query_finished(conn, err=e)
            raise e

    async def perform_async(
        self, conn: AioConnection, query_params: dict = None,
        response_config: list = None, **kwargs,
    ) -> APIResult:
        """
        Perform query and process result.

        :param conn: connection to Ignite server,
        :param query_params: (optional) dict of named query parameters.
         Defaults to no parameters,
        :param response_config: (optional) response configuration − list of
         (name, type_hint) tuples. Defaults to empty return value,
        :return: instance of :class:`~pyignite.api.result.APIResult` with raw
         value (may undergo further processing in API functions).
        """
        try:
            self._on_query_started(conn)

            with AioBinaryStream(conn.client) as stream:
                await self.from_python_async(stream, query_params)
                data = await conn.request(self.query_id, stream.getvalue())

            response_struct = self.response_type(protocol_context=conn.protocol_context,
                                                 following=response_config, **kwargs)

            with AioBinaryStream(conn.client, data) as stream:
                response_ctype = await response_struct.parse_async(stream)
                response = stream.read_ctype(response_ctype, direction=READ_BACKWARD)

            result = self.__post_process_response(conn, response_struct, response)
            if result.status == 0:
                result.value = await response_struct.to_python_async(response)
            self._on_query_finished(conn, result=result)
            return result
        except Exception as e:
            self._on_query_finished(conn, err=e)
            raise e

    @staticmethod
    def __post_process_response(conn, response_struct, response):
        if getattr(response, 'flags', False) & RHF_TOPOLOGY_CHANGED:
            # update latest affinity version
            new_affinity = (response.affinity_version, response.affinity_minor)
            old_affinity = conn.client.affinity_version

            if new_affinity > old_affinity:
                conn.client.affinity_version = new_affinity

        # build result
        return APIResult(response)

    @staticmethod
    def _enabled_query_listener(conn):
        client = conn.client
        return client._event_listeners and client._event_listeners.enabled_query_listener

    @staticmethod
    def _event_listener(conn):
        return conn.client._event_listeners

    def _on_query_started(self, conn):
        self._start_ts = time.monotonic()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Start query(query_id=%d, op_type=%s, host=%s, port=%d, node_id=%s)",
                         self.query_id, _get_op_code_name(self.op_code), conn.host, conn.port, conn.uuid)

        if self._enabled_query_listener(conn):
            self._event_listener(conn).publish_query_start(conn.host, conn.port, conn.uuid, self.query_id,
                                                           self.op_code, _get_op_code_name(self.op_code))

    def _on_query_finished(self, conn, result=None, err=None):
        dur_ms = _sec_to_millis(time.monotonic() - self._start_ts)
        if result and result.status != 0:
            err = result.message
        if err:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Failed to perform query(query_id=%d, op_type=%s, host=%s, port=%d, node_id=%s) "
                             "in %d ms: %s", self.query_id, _get_op_code_name(self.op_code),
                             conn.host, conn.port, conn.uuid, dur_ms, err)
            if self._enabled_query_listener(conn):
                self._event_listener(conn).publish_query_fail(conn.host, conn.port, conn.uuid, self.query_id,
                                                              self.op_code, _get_op_code_name(self.op_code),
                                                              dur_ms, err)
        else:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Finished query(query_id=%d, op_type=%s, host=%s, port=%d, node_id=%s) "
                             "successfully in %d ms", self.query_id, _get_op_code_name(self.op_code),
                             conn.host, conn.port, conn.uuid, dur_ms)
            if self._enabled_query_listener(conn):
                self._event_listener(conn).publish_query_success(conn.host, conn.port, conn.uuid, self.query_id,
                                                                 self.op_code, _get_op_code_name(self.op_code),
                                                                 dur_ms)


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

    def _build_header(self, stream):
        header = super()._build_header(stream)
        header.config_length = header.length - ctypes.sizeof(type(header))
        return header
