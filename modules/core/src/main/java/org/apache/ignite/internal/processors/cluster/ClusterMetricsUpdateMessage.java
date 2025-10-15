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
import java.util.HashMap;
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
    /** Mesage type code. */
    public static final short TYPE_CODE = 133;

    /** Node metrics snapshot. */
    @Order(0)
    @Nullable private ClusterMetricsSnapshot nodeMetrics;

    /** Caches metrics wrapper mesage. */
    @Order(1)
    @Nullable private CacheMetricsMessage cacheMetricsMsg;

    /** All-nodes metrics snapshots. */
    @Order(2)
    @Nullable private Map<UUID, ClusterMetricsSnapshot> allNodesMetrics;

    /** All-nodes caches metrics snapshot wrapper messages. */
    @Order(3)
    @Nullable private Map<UUID, CacheMetricsMessage> allCachesMetrics;

    /** Constructor. */
    public ClusterMetricsUpdateMessage() {
        // No-op.
    }

    /** Constructor. */
    public ClusterMetricsUpdateMessage(ClusterMetrics nodeMetrics, Map<Integer, CacheMetrics> cacheMetrics) {
        this.nodeMetrics = ClusterMetricsSnapshot.of(nodeMetrics);
        this.cacheMetricsMsg = new CacheMetricsMessage(cacheMetrics);
    }

    /** Constructor. */
    public ClusterMetricsUpdateMessage(Map<UUID, ClusterNodeMetricsMessage> allNodesMetrics) {
        this.allNodesMetrics = new HashMap<>(allNodesMetrics.size(), 1.0f);
        allCachesMetrics = new HashMap<>(allNodesMetrics.size(), 1.0f);

        allNodesMetrics.forEach((id, metrics)->{
            this.allNodesMetrics.put(id, ClusterMetricsSnapshot.of());
        });


            this.allNodesMetrics = allNodesMetrics.entrySet().stream()
            .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), ClusterMetricsSnapshot.of(e.getValue().nodeMetrics())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        this.allCachesMetrics = allNodesMetrics.entrySet().stream()
            .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), new CacheMetricsMessage(e.getValue().cacheMetrics())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));;
    }

    /** @return Node metrics snapshot. */
    public @Nullable ClusterMetricsSnapshot nodeMetrics() {
        return nodeMetrics;
    }

    /** @param nodeMetrics Node metrics snapshot. */
    public void nodeMetrics(@Nullable ClusterMetricsSnapshot nodeMetrics) {
        this.nodeMetrics = nodeMetrics;
    }

    /** @return Caches metrics wrapper mesage. */
    public @Nullable CacheMetricsMessage cacheMetricsMsg() {
        return cacheMetricsMsg;
    }

    /** @param cacheMetricsMsg Caches metrics wrapper mesage. */
    public void cacheMetricsMsg(CacheMetricsMessage cacheMetricsMsg) {
        this.cacheMetricsMsg = cacheMetricsMsg;
    }

    /** @return All-nodes metrics snapshots. */
    public @Nullable Map<UUID, ClusterMetricsSnapshot> allNodesMetrics() {
        return allNodesMetrics;
    }

    /** @param allNodesMetrics All-nodes metrics snapshots. */
    public void allNodesMetrics(@Nullable Map<UUID, ClusterMetricsSnapshot> allNodesMetrics) {
        this.allNodesMetrics = allNodesMetrics;
    }

    /** @return All-nodes caches metrics snapshot wrapper messages. */
    public @Nullable Map<UUID, CacheMetricsMessage> allCachesMetrics() {
        return allCachesMetrics;
    }

    /** @param allCachesMetrics All-nodes caches metrics snapshot wrapper messages. */
    public void allCachesMetrics(@Nullable Map<UUID, CacheMetricsMessage> allCachesMetrics) {
        this.allCachesMetrics = allCachesMetrics;
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
