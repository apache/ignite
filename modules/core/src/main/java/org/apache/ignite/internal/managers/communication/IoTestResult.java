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

package org.apache.ignite.internal.managers.communication;

import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/** Result of a communication IO test. */
public class IoTestResult {
    /** Source node ID. */
    private final UUID sourceNodeId;

    /** Source node consistent ID. */
    @Nullable
    private final String sourceConsistentId;

    /** Warmup duration in milliseconds. */
    private final long warmupMillis;

    /** Test duration in milliseconds. */
    private final long durationMillis;

    /** Worker thread count. */
    private final int threads;

    /** Payload size in bytes. */
    private final int payloadSize;

    /** Process messages in NIO threads. */
    private final boolean processInNioThread;

    /** Per-target results, sorted by node ID. */
    private final List<TargetResult> targets;

    /** Constructor. */
    IoTestResult(
        UUID sourceNodeId,
        @Nullable String sourceConsistentId,
        long warmupMillis,
        long durationMillis,
        int threads,
        int payloadSize,
        boolean processInNioThread,
        List<TargetResult> targets
    ) {
        this.sourceNodeId = sourceNodeId;
        this.sourceConsistentId = sourceConsistentId;
        this.warmupMillis = warmupMillis;
        this.durationMillis = durationMillis;
        this.threads = threads;
        this.payloadSize = payloadSize;
        this.processInNioThread = processInNioThread;
        this.targets = targets;
    }

    /** @return Source node ID. */
    public UUID sourceNodeId() {
        return sourceNodeId;
    }

    /** @return Source node consistent ID. */
    @Nullable public String sourceConsistentId() {
        return sourceConsistentId;
    }

    /** @return Warmup duration in milliseconds. */
    public long warmupMillis() {
        return warmupMillis;
    }

    /** @return Test duration in milliseconds. */
    public long durationMillis() {
        return durationMillis;
    }

    /** @return Worker thread count. */
    public int threads() {
        return threads;
    }

    /** @return Payload size in bytes. */
    public int payloadSize() {
        return payloadSize;
    }

    /** @return {@code True} when messages are processed in NIO threads. */
    public boolean processInNioThread() {
        return processInNioThread;
    }

    /** @return Per-target results, sorted by node ID. */
    public List<TargetResult> targets() {
        return targets;
    }

    /** Immutable result for one target node. */
    public static class TargetResult {
        /** Target node ID. */
        private final UUID nodeId;

        /** Target node consistent ID. */
        @Nullable
        private final String consistentId;

        /** Sample count. */
        private final long samples;

        /** Minimum RTT in nanoseconds. */
        private final long minRttNanos;

        /** Average RTT in nanoseconds. */
        private final double avgRttNanos;

        /** Maximum RTT in nanoseconds. */
        private final long maxRttNanos;

        /** Request delivery statistics. */
        private final LatencySummary reqDelivery;

        /** Response delivery statistics. */
        private final LatencySummary resDelivery;

        /** Constructor. */
        TargetResult(
            UUID nodeId,
            @Nullable String consistentId,
            long samples,
            long minRttNanos,
            double avgRttNanos,
            long maxRttNanos,
            LatencySummary reqDelivery,
            LatencySummary resDelivery
        ) {
            this.nodeId = nodeId;
            this.consistentId = consistentId;
            this.samples = samples;
            this.minRttNanos = minRttNanos;
            this.avgRttNanos = avgRttNanos;
            this.maxRttNanos = maxRttNanos;
            this.reqDelivery = reqDelivery;
            this.resDelivery = resDelivery;
        }

        /** @return Target node ID. */
        public UUID nodeId() {
            return nodeId;
        }

        /** @return Target node consistent ID. */
        @Nullable public String consistentId() {
            return consistentId;
        }

        /** @return Number of collected samples. */
        public long samples() {
            return samples;
        }

        /** @return Minimum RTT in nanoseconds. */
        public long minimumRttNanos() {
            return minRttNanos;
        }

        /** @return Average RTT in nanoseconds. */
        public double averageRttNanos() {
            return avgRttNanos;
        }

        /** @return Maximum RTT in nanoseconds. */
        public long maximumRttNanos() {
            return maxRttNanos;
        }

        /** @return Request delivery statistics. */
        public LatencySummary requestDelivery() {
            return reqDelivery;
        }

        /** @return Response delivery statistics. */
        public LatencySummary responseDelivery() {
            return resDelivery;
        }
    }

    /** Immutable min/average/max wall-clock delivery latency. */
    public static final class LatencySummary {
        /** Minimum latency in milliseconds. */
        private final long minMillis;

        /** Average latency in milliseconds. */
        private final double avgMillis;

        /** Maximum latency in milliseconds. */
        private final long maxMillis;

        /** Constructor. */
        LatencySummary(long minMillis, double avgMillis, long maxMillis) {
            this.minMillis = minMillis;
            this.avgMillis = avgMillis;
            this.maxMillis = maxMillis;
        }

        /** @return Minimum latency in milliseconds. */
        public long minimumMillis() {
            return minMillis;
        }

        /** @return Average latency in milliseconds. */
        public double averageMillis() {
            return avgMillis;
        }

        /** @return Maximum latency in milliseconds. */
        public long maximumMillis() {
            return maxMillis;
        }
    }
}
