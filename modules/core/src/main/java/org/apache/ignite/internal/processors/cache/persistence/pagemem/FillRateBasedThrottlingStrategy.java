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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesWriteThrottlePolicy.CP_BUF_FILL_THRESHOLD;

/**
 * Logic used to protect memory (Checkpoint Buffer) from exhaustion using throttling duration based on storage fill rate.
 */
class FillRateBasedThrottlingStrategy implements ThrottlingStrategy {
    /**
     * Minimum throttle time. 10 microseconds.
     */
    private static final long MIN_THROTTLE_NANOS = 10_000L;

    /**
     * Maximum throttle time. 1 second.
     */
    private static final long MAX_THROTTLE_NANOS = 1_000_000_000L;

    /**
     * The exponent to calculate park time.
     */
    private static final double POW = Math.log((double)MAX_THROTTLE_NANOS / MIN_THROTTLE_NANOS);

    /** */
    private final CheckpointBufferOverflowWatchdog cpBufOverflowWatchdog;

    /** */
    private final AtomicBoolean throttlingStarted = new AtomicBoolean();

    /** */
    FillRateBasedThrottlingStrategy(CheckpointBufferOverflowWatchdog watchdog) {
        cpBufOverflowWatchdog = watchdog;
    }

    /** {@inheritDoc} */
    @Override public long protectionParkTime() {
        double fillRate = cpBufOverflowWatchdog.fillRate();

        if (fillRate < CP_BUF_FILL_THRESHOLD)
            return 0;

        throttlingStarted.set(true);

        return (long)(Math.exp(POW * (fillRate - CP_BUF_FILL_THRESHOLD) / (1 - CP_BUF_FILL_THRESHOLD)) * MIN_THROTTLE_NANOS);
    }

    /** {@inheritDoc} */
    @Override public boolean reset() {
        return throttlingStarted.compareAndSet(true, false);
    }
}
