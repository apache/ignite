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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Holds map of thick client or server metrics messages per node id. */
public class TcpDiscoveryClientNodesMetricsMessage implements Message {
    /** Map of nodes metrics messages per node id. */
    @Order(0)
    Map<UUID, TcpDiscoveryNodeMetricsMessage> nodesMetricsMsgs;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public TcpDiscoveryClientNodesMetricsMessage() {
        // No-op.
    }

    /** @return Map of nodes metrics messages per node id. */
    public Map<UUID, TcpDiscoveryNodeMetricsMessage> nodesMetricsMessages() {
        return nodesMetricsMsgs;
    }

    /** @param nodesMetricsMsgs Map of nodes metrics messages per node id. */
    public void nodesMetricsMessages(Map<UUID, TcpDiscoveryNodeMetricsMessage> nodesMetricsMsgs) {
        this.nodesMetricsMsgs = nodesMetricsMsgs;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -104;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryClientNodesMetricsMessage.class, this, "super", super.toString());
    }
}
