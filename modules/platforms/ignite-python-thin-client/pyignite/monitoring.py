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

""" Tools to monitor client's events.

For example, a simple query logger might be implemented like this::

    import logging

    from pyignite import monitoring

    class QueryLogger(monitoring.QueryEventListener):

        def on_query_start(self, event):
            logging.info(f"Query {event.op_name} with query id "
                         f"{event.query_id} started on server "
                         f"{event.host}:{event.port}")

        def on_query_fail(self, event):
            logging.info(f"Query {event.op_name} with query id "
                         f"{event.query_id} on server "
                         f"{event.host}:{event.port} "
                         f"failed in {event.duration}ms "
                         f"with error {event.error_msg}")

        def on_query_success(self, event):
            logging.info(f"Query {event.op_name} with query id "
                         f"{event.query_id} on server " \
                         f"{event.host}:{event.port} " \
                         f"succeeded in {event.duration}ms")

:class:`~ConnectionEventListener` is also available.

Event listeners can be registered by passing parameter to :class:`~pyignite.client.Client` or
:class:`~pyignite.aio_client.AioClient` constructor::

    client = Client(event_listeners=[QueryLogger()])
    with client.connect('127.0.0.1', 10800):
        ....

.. note:: Events are delivered **synchronously**. Application threads block
  waiting for event handlers. Care must be taken to ensure that your event handlers are efficient
  enough to not adversely affect overall application performance.

.. note:: Debug logging is also available, standard ``logging`` is used. Just set ``DEBUG`` level to
    *pyignite* logger.
"""
from typing import Optional, Sequence


class _BaseEvent:
    def __init__(self, **kwargs):
        if kwargs:
            for k, v in kwargs.items():
                object.__setattr__(self, k, v)

    def __setattr__(self, name, value):
        raise TypeError(f'{self.__class__.__name__} is immutable')

    def __repr__(self):
        pass


class _ConnectionEvent(_BaseEvent):
    __slots__ = ('host', 'port')
    host: str
    port: int

    def __init__(self, host, port, **kwargs):
        super().__init__(host=host, port=port, **kwargs)


class _HandshakeEvent(_ConnectionEvent):
    __slots__ = ('protocol_context',)
    protocol_context: Optional['ProtocolContext']

    def __init__(self, host, port, protocol_context=None, **kwargs):
        super().__init__(host, port, protocol_context=protocol_context.copy() if protocol_context else None, **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(host={self.host}, port={self.port}, " \
               f"protocol_context={self.protocol_context})"


class HandshakeStartEvent(_HandshakeEvent):
    """
    Published when a handshake started.

    :ivar host: Address of the node to connect,
    :ivar port: Port number of the node to connect,
    :ivar protocol_context: Client's protocol context.
    """
    def __init__(self, host, port, protocol_context=None, **kwargs):
        """
        This class is not supposed to be constructed by user.
        """
        super().__init__(host, port, protocol_context, **kwargs)


class HandshakeFailedEvent(_HandshakeEvent):
    """
    Published when a handshake failed.

    :ivar host: Address of the node to connect,
    :ivar port: Port number of the node to connect,
    :ivar protocol_context: Client's protocol context,
    :ivar error_msg: Error message.
    """
    __slots__ = ('error_msg',)
    error_msg: str

    def __init__(self, host, port, protocol_context=None, err=None, **kwargs):
        """
        This class is not supposed to be constructed by user.
        """
        super().__init__(host, port, protocol_context, error_msg=repr(err) if err else '', **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(host={self.host}, port={self.port}, " \
               f"protocol_context={self.protocol_context}, error_msg={self.error_msg})"


class AuthenticationFailedEvent(HandshakeFailedEvent):
    """
    Published when an authentication is failed.

    :ivar host: Address of the node to connect,
    :ivar port: Port number of the node to connect,
    :ivar protocol_context: Client protocol context,
    :ivar error_msg: Error message.
    """
    pass


class HandshakeSuccessEvent(_HandshakeEvent):
    """
    Published when a handshake succeeded.

    :ivar host: Address of the node to connect,
    :ivar port: Port number of the node to connect,
    :ivar protocol_context: Client's protocol context,
    :ivar node_uuid: Node's uuid, string.
    """
    __slots__ = ('node_uuid',)
    node_uuid: str

    def __init__(self, host, port, protocol_context, node_uuid, **kwargs):
        """
        This class is not supposed to be constructed by user.
        """
        super().__init__(host, port, protocol_context, node_uuid=str(node_uuid) if node_uuid else '', **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(host={self.host}, port={self.port}, " \
               f"node_uuid={self.node_uuid}, protocol_context={self.protocol_context})"


class ConnectionClosedEvent(_ConnectionEvent):
    """
    Published when a connection to the node is expectedly closed.

    :ivar host: Address of node to connect,
    :ivar port: Port number of node to connect,
    :ivar node_uuid: Node uuid, string.
    """
    __slots__ = ('node_uuid',)
    node_uuid: str

    def __init__(self, host, port, node_uuid, **kwargs):
        """
        This class is not supposed to be constructed by user.
        """
        super().__init__(host, port, node_uuid=str(node_uuid) if node_uuid else '', **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(host={self.host}, port={self.port}, node_uuid={self.node_uuid})"


class ConnectionLostEvent(ConnectionClosedEvent):
    """
    Published when a connection to the node is lost.

    :ivar host: Address of the node to connect,
    :ivar port: Port number of the node to connect,
    :ivar node_uuid: Node's uuid, string,
    :ivar error_msg: Error message.
    """
    __slots__ = ('error_msg',)
    node_uuid: str
    error_msg: str

    def __init__(self, host, port, node_uuid, err, **kwargs):
        """
        This class is not supposed to be constructed by user.
        """
        super().__init__(host, port, node_uuid, error_msg=repr(err) if err else '', **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(host={self.host}, port={self.port}, " \
               f"node_uuid={self.node_uuid}, error_msg={self.error_msg})"


class _EventListener:
    pass


class ConnectionEventListener(_EventListener):
    """
    Base class for connection event listeners.
    """
    def on_handshake_start(self, event: HandshakeStartEvent):
        """
        Handle handshake start event.

        :param event: Instance of :class:`HandshakeStartEvent`.
        """
        pass

    def on_handshake_success(self, event: HandshakeSuccessEvent):
        """
        Handle handshake success event.

        :param event: Instance of :class:`HandshakeSuccessEvent`.
        """
        pass

    def on_handshake_fail(self, event: HandshakeFailedEvent):
        """
        Handle handshake failed event.

        :param event: Instance of :class:`HandshakeFailedEvent`.
        """
        pass

    def on_authentication_fail(self, event: AuthenticationFailedEvent):
        """
        Handle authentication failed event.

        :param event: Instance of :class:`AuthenticationFailedEvent`.
        """
        pass

    def on_connection_closed(self, event: ConnectionClosedEvent):
        """
        Handle connection closed event.

        :param event: Instance of :class:`ConnectionClosedEvent`.
        """
        pass

    def on_connection_lost(self, event: ConnectionLostEvent):
        """
        Handle connection lost event.

        :param event: Instance of :class:`ConnectionLostEvent`.
        """
        pass


class _QueryEvent(_BaseEvent):
    __slots__ = ('host', 'port', 'node_uuid', 'query_id', 'op_code', 'op_name')
    host: str
    port: int
    node_uuid: str
    query_id: int
    op_code: int
    op_name: str

    def __init__(self, host, port, node_uuid, query_id, op_code, op_name, **kwargs):
        """
        This class is not supposed to be constructed by user.
        """
        super().__init__(host=host, port=port, node_uuid=str(node_uuid) if node_uuid else '',
                         query_id=query_id, op_code=op_code, op_name=op_name, **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(host={self.host}, port={self.port}, " \
               f"node_uuid={self.node_uuid}, query_id={self.query_id}, " \
               f"op_code={self.op_code}, op_name={self.op_name})"


class QueryStartEvent(_QueryEvent):
    """
    Published when a client's query started.

    :ivar host: Address of the node on which the query is executed,
    :ivar port: Port number of the node on which the query is executed,
    :ivar node_uuid: Node's uuid, string,
    :ivar query_id: Query's id,
    :ivar op_code: Operation's id,
    :ivar op_name: Operation's name.
    """
    pass


class QuerySuccessEvent(_QueryEvent):
    """
    Published when a client's query finished successfully.

    :ivar host: Address of the node on which the query is executed,
    :ivar port: Port number of the node on which the query is executed,
    :ivar node_uuid: Node's uuid, string,
    :ivar query_id: Query's id,
    :ivar op_code: Operation's id,
    :ivar op_name: Operation's name,
    :ivar duration: Query's duration in milliseconds.
    """
    __slots__ = ('duration', )
    duration: int

    def __init__(self, host, port, node_uuid, query_id, op_code, op_name, duration, **kwargs):
        super().__init__(host, port, node_uuid, query_id, op_code, op_name, duration=duration, **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(host={self.host}, port={self.port}, " \
               f"node_uuid={self.node_uuid}, query_id={self.query_id}, " \
               f"op_code={self.op_code}, op_name={self.op_name}, duration={self.duration})"


class QueryFailEvent(_QueryEvent):
    """
    Published when a client's query failed.

    :ivar host: Address of the node on which the query is executed,
    :ivar port: Port number of the node on which the query is executed,
    :ivar node_uuid: Node's uuid, string,
    :ivar query_id: Query's id,
    :ivar op_code: Operation's id,
    :ivar op_name: Operation's name,
    :ivar duration: Query's duration in milliseconds,
    :ivar error_msg: Error message.
    """
    __slots__ = ('duration', 'err_msg')
    duration: int
    err_msg: str

    def __init__(self, host, port, node_uuid, query_id, op_code, op_name, duration, err, **kwargs):
        super().__init__(host, port, node_uuid, query_id, op_code, op_name, duration=duration,
                         err_msg=repr(err) if err else '', **kwargs)

    def __repr__(self):
        return f"{self.__class__.__name__}(host={self.host}, port={self.port}, " \
               f"node_uuid={self.node_uuid}, query_id={self.query_id}, op_code={self.op_code}, " \
               f"op_name={self.op_name}, duration={self.duration}, err_msg={self.err_msg})"


class QueryEventListener(_EventListener):
    """
    Base class for query event listeners.
    """
    def on_query_start(self, event: QueryStartEvent):
        """
        Handle query start event.

        :param event: Instance of :class:`QueryStartEvent`.
        """
        pass

    def on_query_success(self, event: QuerySuccessEvent):
        """
        Handle query success event.

        :param event: Instance of :class:`QuerySuccessEvent`.
        """
        pass

    def on_query_fail(self, event: QueryFailEvent):
        """
        Handle query fail event.

        :param event: Instance of :class:`QueryFailEvent`.
        """
        pass


class _EventListeners:
    def __init__(self, listeners: Optional[Sequence]):
        self.__connection_listeners = []
        self.__query_listeners = []
        if listeners:
            for listener in listeners:
                if isinstance(listener, ConnectionEventListener):
                    self.__connection_listeners.append(listener)
                elif isinstance(listener, QueryEventListener):
                    self.__query_listeners.append(listener)

    @property
    def enabled_connection_listener(self):
        return bool(self.__connection_listeners)

    @property
    def enabled_query_listener(self):
        return bool(self.__query_listeners)

    def publish_handshake_start(self, host, port, protocol_context):
        evt = HandshakeStartEvent(host, port, protocol_context)
        self.__publish_connection_events(lambda listener: listener.on_handshake_start(evt))

    def publish_handshake_success(self, host, port, protocol_context, node_uuid):
        evt = HandshakeSuccessEvent(host, port, protocol_context, node_uuid)
        self.__publish_connection_events(lambda listener: listener.on_handshake_success(evt))

    def publish_handshake_fail(self, host, port, protocol_context, err):
        evt = HandshakeFailedEvent(host, port, protocol_context, err)
        self.__publish_connection_events(lambda listener: listener.on_handshake_fail(evt))

    def publish_authentication_fail(self, host, port, protocol_context, err):
        evt = AuthenticationFailedEvent(host, port, protocol_context, err)
        self.__publish_connection_events(lambda listener: listener.on_authentication_fail(evt))

    def publish_connection_closed(self, host, port, node_uuid):
        evt = ConnectionClosedEvent(host, port, node_uuid)
        self.__publish_connection_events(lambda listener: listener.on_connection_closed(evt))

    def publish_connection_lost(self, host, port, node_uuid, err):
        evt = ConnectionLostEvent(host, port, node_uuid, err)
        self.__publish_connection_events(lambda listener: listener.on_connection_lost(evt))

    def publish_query_start(self, host, port, node_uuid, query_id, op_code, op_name):
        evt = QueryStartEvent(host, port, node_uuid, query_id, op_code, op_name)
        self.__publish_query_events(lambda listener: listener.on_query_start(evt))

    def publish_query_success(self, host, port, node_uuid, query_id, op_code, op_name, duration):
        evt = QuerySuccessEvent(host, port, node_uuid, query_id, op_code, op_name, duration)
        self.__publish_query_events(lambda listener: listener.on_query_success(evt))

    def publish_query_fail(self, host, port, node_uuid, query_id, op_code, op_name, duration, err):
        evt = QueryFailEvent(host, port, node_uuid, query_id, op_code, op_name, duration, err)
        self.__publish_query_events(lambda listener: listener.on_query_fail(evt))

    def __publish_connection_events(self, callback):
        try:
            for listener in self.__connection_listeners:
                callback(listener)
        except: # noqa: 13
            pass

    def __publish_query_events(self, callback):
        try:
            for listener in self.__query_listeners:
                callback(listener)
        except: # noqa: 13
            pass
