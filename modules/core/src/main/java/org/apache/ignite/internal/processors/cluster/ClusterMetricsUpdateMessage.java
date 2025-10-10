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

package org.apache.ignite.internal.processors.cluster;

import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public final class ClusterMetricsUpdateMessage implements Message {
    /** */
    public static final short TYPE_CODE = 133;

    /** */
    @Nullable private ClusterMetricsSnapshot nodeMetrics;

    /** */
    @Order(0)
    @Nullable private Map<UUID, ClusterMetricsSnapshot> allNodesMetrics;

    /** */
    private Map<Integer, CacheMetrics> cacheMetrics;

    /** */
    public ClusterMetricsUpdateMessage() {
        // No-op.
    }

    /** */
    public ClusterMetricsUpdateMessage(ClusterMetrics nodeMetrics, Map<Integer, CacheMetrics> cacheMetrics) {
        this.nodeMetrics = metricsSnapshot(nodeMetrics);

        this.cacheMetrics = cacheMetrics;
    }

    /** */
    private ClusterMetricsUpdateMessage(Map<UUID, ClusterMetricsSnapshot> allNodesMetrics) {
        this.allNodesMetrics = allNodesMetrics;
    }

    /** */
    public static ClusterMetricsUpdateMessage of(Map<UUID, ? extends ClusterMetrics> allNodesMetrics) {
        Map<UUID, ClusterMetricsSnapshot> allNodesMetrics0 = allNodesMetrics.entrySet().stream()
            .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), metricsSnapshot(e.getValue())))
            .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        return new ClusterMetricsUpdateMessage(allNodesMetrics0);
    }

    /** */
    private static ClusterMetricsSnapshot metricsSnapshot(ClusterMetrics metrics) {
        return metrics instanceof ClusterMetricsSnapshot ? (ClusterMetricsSnapshot)metrics : new ClusterMetricsSnapshot(metrics);
    }

    /**
     * @return Node metrics.
     */
    public @Nullable ClusterMetricsSnapshot nodeMetrics() {
        return nodeMetrics;
    }

    /**
     * @return All nodes metrics.
     */
    public @Nullable Map<UUID, ClusterMetricsSnapshot> allNodesMetrics() {
        return allNodesMetrics;
    }

    /** */
    public @Nullable Map<Integer, CacheMetrics> cacheMetrics() {
        return cacheMetrics;
    }

    /** */
    public void allNodesMetrics(@Nullable Map<UUID, ClusterMetricsSnapshot> allNodesMetrics) {
        this.allNodesMetrics = allNodesMetrics;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterMetricsUpdateMessage.class, this);
    }
}
