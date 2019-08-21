/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.processors.tracing.Traces;
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
    private static final Map<Class<? extends TraceableMessage>, String> msgTraceLookupTable = new ConcurrentHashMap<>();

    static {
        msgTraceLookupTable.put(TcpDiscoveryJoinRequestMessage.class, Traces.Discovery.NODE_JOIN_REQUEST);
        msgTraceLookupTable.put(TcpDiscoveryNodeAddedMessage.class, Traces.Discovery.NODE_JOIN_ADD);
        msgTraceLookupTable.put(TcpDiscoveryNodeAddFinishedMessage.class, Traces.Discovery.NODE_JOIN_FINISH);
        msgTraceLookupTable.put(TcpDiscoveryNodeFailedMessage.class, Traces.Discovery.NODE_FAILED);
        msgTraceLookupTable.put(TcpDiscoveryNodeLeftMessage.class, Traces.Discovery.NODE_LEFT);
        msgTraceLookupTable.put(TcpDiscoveryCustomEventMessage.class, Traces.Discovery.CUSTOM_EVENT);
        msgTraceLookupTable.put(TcpDiscoveryServerOnlyCustomEventMessage.class, Traces.Discovery.CUSTOM_EVENT);
    }

    /** */
    private TraceableMessagesTable() {};

    /**
     * @param msgCls Traceable message class.
     * @return Trace name associated with message with given class.
     */
    public static String traceName(Class<? extends TraceableMessage> msgCls) {
        String traceName = msgTraceLookupTable.get(msgCls);

        if (traceName == null)
            throw new IgniteException("Trace name is not defined for " + msgCls);

        return traceName;
    }
}
