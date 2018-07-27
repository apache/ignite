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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Metrics update message.
 * <p>
 * Client sends his metrics in this message.
 */
public class TcpDiscoveryClientMetricsUpdateMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final byte[] metrics;

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node.
     * @param metrics Metrics.
     */
    public TcpDiscoveryClientMetricsUpdateMessage(UUID creatorNodeId, ClusterMetrics metrics,
        Map<Integer, CacheMetrics> cacheMetrics) {
        super(creatorNodeId);

        this.metrics = ClusterMetricsSnapshot.serialize(metrics);

        ByteArrayOutputStream byteStream= new ByteArrayOutputStream();
        ObjectOutputStream objectStream;

        try {
            objectStream = new ObjectOutputStream(byteStream);

            objectStream.writeObject(cacheMetrics);
        }
        catch (IOException e) {
            //No-op.
        }

        byte[] src = byteStream.toByteArray();

        U.arrayCopy(src, 0, this.metrics, ClusterMetricsSnapshot.METRICS_SIZE, src.length);
    }

    public Map<Integer, CacheMetrics> cacheMetrics() {

        ByteArrayInputStream s = new ByteArrayInputStream(this.metrics, 16 + ClusterMetricsSnapshot.METRICS_SIZE, 1);

        Map<Integer, CacheMetrics> l = null;

        try {
            ObjectInputStream s2 = new ObjectInputStream(s);

            l = (Map<Integer, CacheMetrics>)s2.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }


        return l;
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