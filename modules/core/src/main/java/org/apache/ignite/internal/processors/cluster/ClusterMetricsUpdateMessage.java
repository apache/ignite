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
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public final class ClusterMetricsUpdateMessage implements Message {
    /** */
    public static final short TYPE_CODE = 133;

    /** Single node metrics message. */
    @Order(0)
    @Nullable NodeFullMetricsMessage singleNodeMetricsMsg;

    /** All-nodes cache metrics messages. */
    @Order(1)
    @Nullable Map<UUID, NodeFullMetricsMessage> allNodesMetrics;

    /** Default constructor. Required for {@link GridIoMessageFactory}. */
    public ClusterMetricsUpdateMessage() {
        // No-op.
    }

    /** Single node metrics constructor. */
    public ClusterMetricsUpdateMessage(ClusterMetrics nodeMetrics, Map<Integer, CacheMetrics> cacheMetrics) {
        singleNodeMetricsMsg = new NodeFullMetricsMessage(nodeMetrics, cacheMetrics);
    }

    /** All-nodes metrics constructor. */
    public ClusterMetricsUpdateMessage(Map<UUID, ClusterNodeMetrics> allNodesMetrics) {
        this.allNodesMetrics = new HashMap<>(allNodesMetrics.size(), 1.0f);

        allNodesMetrics.forEach((id, e) -> this.allNodesMetrics.put(id, new NodeFullMetricsMessage(e.nodeMetrics(), e.cacheMetrics())));
    }

    /** */
    public @Nullable Map<UUID, NodeFullMetricsMessage> allNodesMetrics() {
        return allNodesMetrics;
    }

    /** */
    public void allNodesMetrics(@Nullable Map<UUID, NodeFullMetricsMessage> allNodesMetrics) {
        assert allNodesMetrics == null || singleNodeMetricsMsg == null;

        this.allNodesMetrics = allNodesMetrics;
    }

    /** */
    public @Nullable NodeFullMetricsMessage singleNodeMetricsMsg() {
        return singleNodeMetricsMsg;
    }

    /** */
    public void singleNodeMetricsMsg(@Nullable NodeFullMetricsMessage singleNodeMetricsMsg) {
        assert singleNodeMetricsMsg == null || allNodesMetrics == null;

        this.singleNodeMetricsMsg = singleNodeMetricsMsg;
    }

    /** */
    public boolean singleNodeMetrics() {
        assert (singleNodeMetricsMsg == null && allNodesMetrics != null) || (singleNodeMetricsMsg != null && allNodesMetrics == null);

        return singleNodeMetricsMsg != null;
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
