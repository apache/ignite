/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.discovery;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Structured result of a Discovery SPI ring IO test. */
public final class IoTestDiscoveryResult {
    /** Coordinator node ID. */
    private final UUID coordinatorNodeId;

    /** Consistent IDs captured for the server nodes participating in this result. */
    private final Map<UUID, String> nodeConsistentIds;

    /** Ring latency aggregate. */
    private final RingLatencySummary ringLatency;

    /** Per-hop latency aggregates in ring order. */
    private final List<HopLatencySummary> hopLatencies;

    /**
     * @param coordinatorNodeId Coordinator node ID.
     * @param nodeConsistentIds Consistent IDs by node ID.
     * @param ringLatency Ring latency aggregate.
     * @param hopLatencies Per-hop latency aggregates.
     */
    public IoTestDiscoveryResult(
        UUID coordinatorNodeId,
        Map<UUID, String> nodeConsistentIds,
        RingLatencySummary ringLatency,
        List<HopLatencySummary> hopLatencies
    ) {
        this.coordinatorNodeId = coordinatorNodeId;
        this.nodeConsistentIds = Collections.unmodifiableMap(new LinkedHashMap<>(nodeConsistentIds));
        this.ringLatency = ringLatency;
        this.hopLatencies = List.copyOf(hopLatencies);
    }

    /** @return Coordinator node ID. */
    public UUID coordinatorNodeId() {
        return coordinatorNodeId;
    }

    /** @return Consistent IDs captured for the server nodes participating in this result. */
    public Map<UUID, String> nodeConsistentIds() {
        return nodeConsistentIds;
    }

    /** @return Ring latency aggregate. */
    public RingLatencySummary ringLatency() {
        return ringLatency;
    }

    /** @return Per-hop latency aggregates in ring order. */
    public List<HopLatencySummary> hopLatencies() {
        return hopLatencies;
    }

    /** Ring latency aggregate. */
    public static final class RingLatencySummary {
        /** Sample count. */
        private final int samples;

        /** Minimum latency in milliseconds. */
        private final double minMillis;

        /** Average latency in milliseconds. */
        private final double averageMillis;

        /** Maximum latency in milliseconds. */
        private final double maxMillis;

        /** */
        public RingLatencySummary(
            int samples,
            double minMillis,
            double averageMillis,
            double maxMillis
        ) {
            this.samples = samples;
            this.minMillis = minMillis;
            this.averageMillis = averageMillis;
            this.maxMillis = maxMillis;
        }

        /** @return Sample count. */
        public int samples() {
            return samples;
        }

        /** @return Minimum latency in milliseconds. */
        public double minMillis() {
            return minMillis;
        }

        /** @return Average latency in milliseconds. */
        public double averageMillis() {
            return averageMillis;
        }

        /** @return Maximum latency in milliseconds. */
        public double maxMillis() {
            return maxMillis;
        }
    }

    /** Per-hop wall-clock latency aggregate. */
    public static final class HopLatencySummary {
        /** Sender node ID. */
        private final UUID fromNodeId;

        /** Receiver node ID. */
        private final UUID toNodeId;

        /** Minimum latency in milliseconds. */
        private final long minMillis;

        /** Average latency in milliseconds. */
        private final double averageMillis;

        /** Maximum latency in milliseconds. */
        private final long maxMillis;

        /** */
        public HopLatencySummary(
            UUID fromNodeId,
            UUID toNodeId,
            long minMillis,
            double averageMillis,
            long maxMillis
        ) {
            this.fromNodeId = fromNodeId;
            this.toNodeId = toNodeId;
            this.minMillis = minMillis;
            this.averageMillis = averageMillis;
            this.maxMillis = maxMillis;
        }

        /** @return Sender node ID. */
        public UUID fromNodeId() {
            return fromNodeId;
        }

        /** @return Receiver node ID. */
        public UUID toNodeId() {
            return toNodeId;
        }

        /** @return Minimum latency in milliseconds. */
        public long minMillis() {
            return minMillis;
        }

        /** @return Average latency in milliseconds. */
        public double averageMillis() {
            return averageMillis;
        }

        /** @return Maximum latency in milliseconds. */
        public long maxMillis() {
            return maxMillis;
        }
    }
}
