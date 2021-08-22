/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.tracing.messages;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryServerOnlyCustomEventMessage;

/**
 * Mapping from traceable message to appropriate trace.
 *
 * @see TraceableMessage inheritors.
 */
public class TraceableMessagesTable {
    /** Message trace lookup table. */
    private static final Map<Class<?>, SpanType> msgTraceLookupTable = new ConcurrentHashMap<>();

    static {
        msgTraceLookupTable.put(TcpDiscoveryJoinRequestMessage.class, SpanType.DISCOVERY_NODE_JOIN_REQUEST);
        msgTraceLookupTable.put(TcpDiscoveryNodeAddedMessage.class, SpanType.DISCOVERY_NODE_JOIN_ADD);
        msgTraceLookupTable.put(TcpDiscoveryNodeAddFinishedMessage.class, SpanType.DISCOVERY_NODE_JOIN_FINISH);
        msgTraceLookupTable.put(TcpDiscoveryNodeFailedMessage.class, SpanType.DISCOVERY_NODE_FAILED);
        msgTraceLookupTable.put(TcpDiscoveryNodeLeftMessage.class, SpanType.DISCOVERY_NODE_LEFT);
        msgTraceLookupTable.put(TcpDiscoveryCustomEventMessage.class, SpanType.DISCOVERY_CUSTOM_EVENT);
        msgTraceLookupTable.put(TcpDiscoveryServerOnlyCustomEventMessage.class, SpanType.DISCOVERY_CUSTOM_EVENT);
        msgTraceLookupTable.put(GridJobExecuteRequest.class, SpanType.COMMUNICATION_JOB_EXECUTE_REQUEST);
        msgTraceLookupTable.put(GridJobExecuteResponse.class, SpanType.COMMUNICATION_JOB_EXECUTE_RESPONSE);
    }

    /** */
    private TraceableMessagesTable() {
    }

    /**
     * @param msgCls Traceable message class.
     * @return Trace name associated with message with given class.
     */
    public static SpanType traceName(Class<? extends TraceableMessage> msgCls) {
        SpanType spanType = msgTraceLookupTable.get(msgCls);

        if (spanType == null)
            throw new IgniteException("Trace name is not defined for " + msgCls);

        return spanType;
    }

    /**
     * @param obj Traceable message object.
     * @return Trace name associated with message with given class.
     */
    public static String traceName(Object obj) {
        if (obj == null)
            return "unknown";

        if (obj instanceof GridIoMessage)
            return traceName(((GridIoMessage)obj).message());

        SpanType val = msgTraceLookupTable.get(obj.getClass());

        return val != null ? val.spanName() : obj.getClass().getSimpleName();
    }
}
