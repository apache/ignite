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

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.ClusterMetricsSnapshot.METRICS_SIZE;

/**
 * Metrics update message.
 * <p>
 * Client sends his metrics in this message.
 */
public class TcpDiscoveryClientMetricsUpdateMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private byte[] metrics;

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node.
     * @param metrics Metrics.
     * @param cacheMetrics cache metrics.
     */
    public TcpDiscoveryClientMetricsUpdateMessage(UUID creatorNodeId, ClusterMetrics metrics,
        Map<Integer, CacheMetrics> cacheMetrics) {
        super(creatorNodeId);
        try {
            byte[] metricsArr = ClusterMetricsSnapshot.serialize(metrics);

            byte[] cacheMetricsArr = U.mapToByteArray(cacheMetrics);

            this.metrics = new byte[metricsArr.length + (cacheMetricsArr != null ? cacheMetricsArr.length : 0)];

            U.arrayCopy(metricsArr, 0, this.metrics, 0, metricsArr.length);

            if (cacheMetricsArr != null)
                U.arrayCopy(cacheMetricsArr, 0, this.metrics, metricsArr.length, cacheMetricsArr.length);
        }
        catch (IOException ignore) {
            assert false;
        }
    }

    /**
     *
     */
    public Map<Integer, CacheMetrics> cacheMetrics() {
        try {
            return U.byteArrayToMap(metrics, METRICS_SIZE, metrics.length - METRICS_SIZE);
        }
        catch (IOException | ClassNotFoundException ignore) {
            assert false;
        }

        return null;
    }

    /**
     * Gets metrics map.
     *
     * @return Metrics map.
     */
    public ClusterMetrics metrics() {
        return ClusterMetricsSnapshot.deserialize(metrics, 0);
    }

    /** {@inheritDoc} */
    @Override public boolean highPriority() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean traceLogLevel() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryClientMetricsUpdateMessage.class, this, "super", super.toString());
    }
}