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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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

    /** Single node metrics wrapper message. */
    @Order(0)
    @Nullable private ClusterMetricsSnapshot nodeMetrics;

    /** Single node cache metrics wrapper message. */
    @Order(1)
    @Nullable private CacheMetricsMessage cacheMetricsMsg;

    /** All-nodes metrics wrapper messages. */
    @Order(2)
    @Nullable private Map<UUID, ClusterMetricsSnapshot> allNodesMetrics;

    /** All-nodes cache metrics wrapper messages. */
    @Order(3)
    @Nullable private Map<UUID, CacheMetricsMessage> allCachesMetrics;

    /** Constructor. */
    public ClusterMetricsUpdateMessage() {
        // No-op.
    }

    /** Single node metrics constructor. */
    public ClusterMetricsUpdateMessage(ClusterMetrics nodeMetrics, Map<Integer, ? extends CacheMetrics> cacheMetrics) {
        this.nodeMetrics = ClusterMetricsSnapshot.of(nodeMetrics);
        this.cacheMetricsMsg = new CacheMetricsMessage(cacheMetrics);
    }

    /** All-nodes metrics constructor. */
    public ClusterMetricsUpdateMessage(Map<UUID, ClusterNodeMetrics> allNodesMetrics) {
        this.allNodesMetrics = new HashMap<>(allNodesMetrics.size(), 1.0f);
        allCachesMetrics = new HashMap<>(allNodesMetrics.size(), 1.0f);

        allNodesMetrics.forEach((id, metrics) -> {
            this.allNodesMetrics.put(id, ClusterMetricsSnapshot.of(metrics.nodeMetrics()));
            allCachesMetrics.put(id, new CacheMetricsMessage(metrics.cacheMetrics()));
        });
    }

    /** @return Single node cache metrics wrapper message. */
    public @Nullable ClusterMetricsSnapshot nodeMetrics() {
        return nodeMetrics;
    }

    /** @param nodeMetrics Single node cache metrics wrapper message. */
    public void nodeMetrics(@Nullable ClusterMetricsSnapshot nodeMetrics) {
        this.nodeMetrics = nodeMetrics;
    }

    /** @return Single node cache metrics wrapper message. */
    public @Nullable CacheMetricsMessage cacheMetricsMsg() {
        return cacheMetricsMsg;
    }

    /** @param cacheMetricsMsg Single node cache metrics wrapper message. */
    public void cacheMetricsMsg(CacheMetricsMessage cacheMetricsMsg) {
        this.cacheMetricsMsg = cacheMetricsMsg;
    }

    /** @return All-nodes metrics wrapper messages. */
    public @Nullable Map<UUID, ClusterMetricsSnapshot> allNodesMetrics() {
        return allNodesMetrics;
    }

    /** @param allNodesMetrics All-nodes metrics wrapper messages. */
    public void allNodesMetrics(@Nullable Map<UUID, ClusterMetricsSnapshot> allNodesMetrics) {
        this.allNodesMetrics = allNodesMetrics;
    }

    /** @return All-nodes cache metrics wrapper messages. */
    public @Nullable Map<UUID, CacheMetricsMessage> allCachesMetrics() {
        return allCachesMetrics;
    }

    /** @param allCachesMetrics All-nodes cache metrics wrapper messages. */
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
