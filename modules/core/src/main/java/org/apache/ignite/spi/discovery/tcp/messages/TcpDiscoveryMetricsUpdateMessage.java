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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.processors.cluster.CacheMetricsMessage;
import org.apache.ignite.internal.processors.cluster.NodeMetricsMessage;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

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
public class TcpDiscoveryMetricsUpdateMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Connected clients metrics: server id -> map of clients metrics. */
    @GridToStringExclude
    @Order(value = 5, method = "connectedClientsMetricsMessages")
    private Map<UUID, TcpDiscoveryNodesMetricsMapMessage> connectedClientsMetricsMsgs;

    /** Servers full metrics: server id -> node metrics + node's caches metrics. */
    @GridToStringExclude
    @Order(value = 7, method = "serversFullMetricsMessages")
    private Map<UUID, TcpDiscoveryNodeFullMetricsMessage> serversFullMetricsMsgs;

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
     * Sets metrics for particular node. Supposed to be called before {@link #addServerCacheMetrics(UUID, Map)}.
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

            srvrFullMetrics.nodeMetricsMessage(new NodeMetricsMessage(newMetrics));

            return srvrFullMetrics;
        });
    }

    /**
     * Sets cache metrics for particular node. Supposed to be called after {@link #addServerCacheMetrics(UUID, Map)}.
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
                newCachesMsgsMap.put(cacheId, new CacheMetricsMessage(cacheMetrics)));

            srvrFullMetrics.cachesMetricsMessages(newCachesMsgsMap);

            return srvrFullMetrics;
        });
    }

    /**
     * Sets metrics for a connected client node.
     *
     * @param srvrId Server node ID.
     * @param clientNodeId Connected client node ID.
     * @param clientMetrics Client metrics.
     */
    public void setClientMetrics(UUID srvrId, UUID clientNodeId, ClusterMetrics clientMetrics) {
        assert srvrId != null;
        assert clientNodeId != null;
        assert clientMetrics != null;

        assert serversFullMetricsMsgs.containsKey(srvrId);

        assert !connectedClientsMetricsMsgs.containsKey(srvrId)
            || connectedClientsMetricsMsgs.get(srvrId).nodesMetricsMessages().get(clientNodeId) == null;

        connectedClientsMetricsMsgs.compute(srvrId, (srvrId0, clientsMetricsMsg) -> {
            if (clientsMetricsMsg == null) {
                clientsMetricsMsg = new TcpDiscoveryNodesMetricsMapMessage();
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

    /** */
    public Map<UUID, TcpDiscoveryNodeFullMetricsMessage> serversFullMetricsMessages() {
        return serversFullMetricsMsgs;
    }

    /** */
    public void serversFullMetricsMessages(Map<UUID, TcpDiscoveryNodeFullMetricsMessage> serversFullMetricsMsgs) {
        this.serversFullMetricsMsgs = serversFullMetricsMsgs;
    }

    /** */
    public Map<UUID, TcpDiscoveryNodesMetricsMapMessage> connectedClientsMetricsMessages() {
        return connectedClientsMetricsMsgs;
    }

    /** */
    public void connectedClientsMetricsMessages(Map<UUID, TcpDiscoveryNodesMetricsMapMessage> connectedClientsMetricsMsgs) {
        this.connectedClientsMetricsMsgs = connectedClientsMetricsMsgs;
    }

    /** {@inheritDoc} */
    @Override public boolean traceLogLevel() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryMetricsUpdateMessage.class, this, "super", super.toString());
    }
}
