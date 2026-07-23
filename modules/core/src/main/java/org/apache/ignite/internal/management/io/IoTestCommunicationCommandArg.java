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

package org.apache.ignite.internal.management.io;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.util.typedef.internal.A;

/** */
public class IoTestCommunicationCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** Maximum number of test threads. */
    private static final int MAX_THREADS = 64;

    /** Maximum number of histogram ranges. */
    private static final int MAX_RANGES_COUNT = 1_000;

    /** Maximum payload size. */
    private static final int MAX_PAYLOAD_SIZE = 1024 * 1024;

    /** Maximum warmup or test duration. */
    private static final long MAX_PHASE_DURATION = TimeUnit.HOURS.toMillis(1);

    /** */
    @Order(0)
    @Argument(description = "Source node ID.")
    UUID nodeId;

    /** */
    @Order(1)
    @Argument(optional = true, description = "Warmup duration (millis, max 1 hour).")
    long warmup = TimeUnit.SECONDS.toMillis(5);

    /** */
    @Order(2)
    @Argument(optional = true, description = "Test duration (millis, max 1 hour).")
    long duration = TimeUnit.SECONDS.toMillis(30);

    /** */
    @Order(3)
    @Argument(optional = true, description = "Number of test threads (max 64).")
    int threads = 1;

    /** */
    @Order(4)
    @Argument(optional = true, description = "RTT histogram upper bound (nanos).")
    long maxLatency = TimeUnit.MILLISECONDS.toNanos(100);

    /** */
    @Order(5)
    @Argument(optional = true, description = "Ranges count for RTT histogram (max 1000).")
    int rangesCnt = 5;

    /** */
    @Order(6)
    @Argument(optional = true, description = "Payload size in each direction (bytes, max 1 MiB).")
    int payloadSize;

    /** */
    @Order(7)
    @Argument(optional = true, description = "Process requests and responses in NIO threads.")
    boolean procFromNioThread;

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** */
    public long warmup() {
        return warmup;
    }

    /** */
    public void warmup(long warmup) {
        A.ensure(warmup >= 0 && warmup <= MAX_PHASE_DURATION,
            "warmup must be between 0 and " + MAX_PHASE_DURATION);

        this.warmup = warmup;
    }

    /** */
    public long duration() {
        return duration;
    }

    /** */
    public void duration(long duration) {
        A.ensure(duration > 0 && duration <= MAX_PHASE_DURATION,
            "duration must be between 1 and " + MAX_PHASE_DURATION);

        this.duration = duration;
    }

    /** */
    public int threads() {
        return threads;
    }

    /** */
    public void threads(int threads) {
        A.ensure(threads > 0 && threads <= MAX_THREADS,
            "threads must be between 1 and " + MAX_THREADS);

        this.threads = threads;
    }

    /** */
    public long maxLatency() {
        return maxLatency;
    }

    /** */
    public void maxLatency(long maxLatency) {
        A.ensure(maxLatency > 0, "maxLatency must be > 0");

        this.maxLatency = maxLatency;
    }

    /** */
    public int rangesCnt() {
        return rangesCnt;
    }

    /** */
    public void rangesCnt(int rangesCnt) {
        A.ensure(rangesCnt > 0 && rangesCnt <= MAX_RANGES_COUNT,
            "rangesCnt must be between 1 and " + MAX_RANGES_COUNT);

        this.rangesCnt = rangesCnt;
    }

    /** */
    public int payloadSize() {
        return payloadSize;
    }

    /** */
    public void payloadSize(int payloadSize) {
        A.ensure(payloadSize >= 0 && payloadSize <= MAX_PAYLOAD_SIZE,
            "payloadSize must be between 0 and " + MAX_PAYLOAD_SIZE);

        this.payloadSize = payloadSize;
    }

    /** */
    public boolean procFromNioThread() {
        return procFromNioThread;
    }

    /** */
    public void procFromNioThread(boolean procFromNioThread) {
        this.procFromNioThread = procFromNioThread;
    }
}
