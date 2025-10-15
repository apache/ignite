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

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.processors.cache.CacheMetricsSnapshot;
import org.apache.ignite.plugin.extensions.communication.Message;

/** */
class ClusterNodeMetricsMessage implements Message {
    /** Message type code. */
    public static final short TYPE_CODE = 138;

    /** Node metrics wrapper message. */
    private ClusterMetricsSnapshot nodeMetrics;

    /** Map of cache metrics wrapper messages. */
    private Map<Integer, CacheMetricsSnapshot> cacheMetrics;

    /**
     * @param nodeMetrics Node metrics.
     * @param cacheMetrics Cache metrics.
     */
    ClusterNodeMetricsMessage(ClusterMetrics nodeMetrics, Map<Integer, CacheMetrics> cacheMetrics) {
        this.nodeMetrics = ClusterMetricsSnapshot.of(nodeMetrics);
        this.cacheMetrics = cacheMetrics;
    }

    /**
     * @return Node metrics.
     */
    ClusterMetricsSnapshot nodeMetrics() {
        return nodeMetrics;
    }

    /**
     * @return Cache metrics.
     */
    Map<Integer, CacheMetricsSnapshot> cacheMetrics() {
        return cacheMetrics != null ? cacheMetrics : Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
