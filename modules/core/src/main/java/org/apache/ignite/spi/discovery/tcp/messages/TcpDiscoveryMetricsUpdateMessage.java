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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.processors.cluster.CacheMetricsMessage;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Metrics update message.
 * <p>
 * It is sent by coordinator node across the ring once a configured period.
 * Message makes two passes:
 * <ol>
 *      <li>During first pass, all nodes add their metrics to the message and
 *          update local metrics with metrics currently present in the message.</li>
 *      <li>During second pass, all nodes update all metrics present in the message
 *          and remove their own metrics from the message.</li>
 * </ol>
 * When message reaches coordinator second time it is discarded (it finishes the
 * second pass).
 */
@TcpDiscoveryRedirectToClient
public class TcpDiscoveryMetricsUpdateMessage extends TcpDiscoveryAbstractMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Connected clients metrics: server id -> client id -> clients metrics. */
    @GridToStringExclude
    @Order(5)
    Map<UUID, TcpDiscoveryClientNodesMetricsMessage> connectedClientsMetricsMsgs;

    /** Servers full metrics: server id -> server metrics + metrics of server's caches. */
    @GridToStringExclude
    @Order(6)
    @Nullable Map<UUID, TcpDiscoveryNodeFullMetricsMessage> serversFullMetricsMsgs;

    /** Client node IDs. */
    @Order(7)
    @Nullable Set<UUID> clientNodeIds;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public TcpDiscoveryMetricsUpdateMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node.
     */
    public TcpDiscoveryMetricsUpdateMessage(UUID creatorNodeId) {
        super(creatorNodeId);
    }

    /**
     * Sets metrics for particular node.
     *
     * @param srvrId Server ID.
     * @param newMetrics New server metrics to add.
     */
    public void addServerMetrics(UUID srvrId, ClusterMetrics newMetrics) {
        assert srvrId != null;
        assert newMetrics != null;

        if (serversFullMetricsMsgs == null)
            serversFullMetricsMsgs = new HashMap<>();

        assert !serversFullMetricsMsgs.containsKey(srvrId);

        serversFullMetricsMsgs.compute(srvrId, (srvrId0, srvrFullMetrics) -> {
            if (srvrFullMetrics == null)
                srvrFullMetrics = new TcpDiscoveryNodeFullMetricsMessage();

            srvrFullMetrics.nodeMetricsMessage(new TcpDiscoveryNodeMetricsMessage(newMetrics));

            return srvrFullMetrics;
        });
    }

    /**
     * Sets cache metrics for particular node.
     *
     * @param srvrId Server ID.
     * @param newCachesMetrics News server's caches metrics to add.
     */
    public void addServerCacheMetrics(UUID srvrId, Map<Integer, CacheMetrics> newCachesMetrics) {
        assert srvrId != null;
        assert newCachesMetrics != null;

        if (serversFullMetricsMsgs == null)
            serversFullMetricsMsgs = new HashMap<>();

        assert serversFullMetricsMsgs.containsKey(srvrId) && serversFullMetricsMsgs.get(srvrId).cachesMetricsMessages() == null;

        serversFullMetricsMsgs.compute(srvrId, (srvrId0, srvrFullMetrics) -> {
            if (srvrFullMetrics == null)
                srvrFullMetrics = new TcpDiscoveryNodeFullMetricsMessage();

            Map<Integer, CacheMetricsMessage> newCachesMsgsMap = U.newHashMap(newCachesMetrics.size());

            newCachesMetrics.forEach((cacheId, cacheMetrics) ->
                newCachesMsgsMap.put(cacheId, new TcpDiscoveryCacheMetricsMessage(cacheMetrics)));

            srvrFullMetrics.cachesMetricsMessages(newCachesMsgsMap);

            return srvrFullMetrics;
        });
    }

    /**
     * Adds metrics for a connected client node.
     *
     * @param srvrId Server node ID.
     * @param clientNodeId Connected client node ID.
     * @param clientMetrics Client metrics.
     */
    public void addClientMetrics(UUID srvrId, UUID clientNodeId, ClusterMetrics clientMetrics) {
        assert srvrId != null;
        assert clientNodeId != null;
        assert clientMetrics != null;

        if (connectedClientsMetricsMsgs == null)
            connectedClientsMetricsMsgs = new HashMap<>();

        assert !connectedClientsMetricsMsgs.containsKey(srvrId)
            || connectedClientsMetricsMsgs.get(srvrId).nodesMetricsMessages().get(clientNodeId) == null;

        connectedClientsMetricsMsgs.compute(srvrId, (srvrId0, clientsMetricsMsg) -> {
            if (clientsMetricsMsg == null) {
                clientsMetricsMsg = new TcpDiscoveryClientNodesMetricsMessage();
                clientsMetricsMsg.nodesMetricsMessages(new HashMap<>());
            }

            clientsMetricsMsg.nodesMetricsMessages().put(clientNodeId, new TcpDiscoveryNodeMetricsMessage(clientMetrics));

            return clientsMetricsMsg;
        });
    }

    /**
     * Removes metrics for particular server node from the message.
     *
     * @param srvrId Server ID.
     */
    public void removeServerMetrics(UUID srvrId) {
        assert srvrId != null;
        assert serversFullMetricsMsgs != null;

        serversFullMetricsMsgs.remove(srvrId);
    }

    /** @return Map of server full metrics messages. */
    public Map<UUID, TcpDiscoveryNodeFullMetricsMessage> serversFullMetricsMessages() {
        return serversFullMetricsMsgs;
    }

    /** @param serversFullMetricsMsgs Map of server full metrics messages. */
    public void serversFullMetricsMessages(Map<UUID, TcpDiscoveryNodeFullMetricsMessage> serversFullMetricsMsgs) {
        this.serversFullMetricsMsgs = serversFullMetricsMsgs;
    }

    /** @return Map of nodes metrics messages. */
    public @Nullable Map<UUID, TcpDiscoveryClientNodesMetricsMessage> connectedClientsMetricsMessages() {
        return connectedClientsMetricsMsgs;
    }

    /** @param connectedClientsMetricsMsgs Map of nodes metrics messages. */
    public void connectedClientsMetricsMessages(Map<UUID, TcpDiscoveryClientNodesMetricsMessage> connectedClientsMetricsMsgs) {
        this.connectedClientsMetricsMsgs = connectedClientsMetricsMsgs;
    }

    /**
     * Gets client node IDs.
     *
     * @return Client node IDs.
     */
    public @Nullable Set<UUID> clientNodeIds() {
        return clientNodeIds;
    }

    /**
     * Sets client node IDs.
     *
     * @param clientNodeIds Client node IDs.
     */
    public void clientNodeIds(@Nullable Set<UUID> clientNodeIds) {
        this.clientNodeIds = clientNodeIds;
    }

    /**
     * Adds client node ID.
     *
     * @param clientNodeId Client node ID.
     */
    public void addClientNodeId(UUID clientNodeId) {
        if (clientNodeIds == null)
            clientNodeIds = new HashSet<>();

        clientNodeIds.add(clientNodeId);
    }

    /** {@inheritDoc} */
    @Override public boolean traceLogLevel() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 14;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryMetricsUpdateMessage.class, this, "super", super.toString());
    }
}
