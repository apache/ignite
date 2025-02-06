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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.lang.IgniteOutClosure;

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

    /** Checkpoint progress provider. */
    private final IgniteOutClosure<CheckpointProgress> cpProgress;

    /** */
    private final AtomicBoolean throttlingStarted = new AtomicBoolean();

    /** */
    FillRateBasedThrottlingStrategy(CheckpointBufferOverflowWatchdog watchdog, IgniteOutClosure<CheckpointProgress> cpProgress) {
        cpBufOverflowWatchdog = watchdog;
        this.cpProgress = cpProgress;
    }

    /** {@inheritDoc} */
    @Override public long protectionParkTime() {
        CheckpointProgress cp = cpProgress.apply();

        // Checkpoint has not been started.
        if (cp == null)
            return 0;

        AtomicInteger cpWrittenRecoveryPagesCounter = cp.writtenRecoveryPagesCounter();
        AtomicInteger cpWrittenPagesCounter = cp.writtenPagesCounter();
        int cpTotalPages = cp.currentCheckpointPagesCount();

        // Checkpoint has been finished.
        if (cpTotalPages == 0 || cpWrittenPagesCounter == null || cpWrittenRecoveryPagesCounter == null)
            return 0;

        // Time to write and fsync all recovery data on checkpoint is close to time of write and fsync all pages to
        // page store, but we don't need to take into account fsync time for data store, since up to this phase
        // checkpoint buffer should be free. So, about 2/3 of time, when checkpoint buffer is widely used, takes
        // recovery data writing, and 1/3 takes writing pages to page store (without fsync).
        double cpProgressRate = (2d * cpWrittenRecoveryPagesCounter.get() + cpWrittenPagesCounter.get()) / 3d / cpTotalPages;
        double cpBufFillRate = cpBufOverflowWatchdog.fillRate();

        if (cpBufFillRate > cpProgressRate && cpProgressRate < 1d) {
            throttlingStarted.set(true);

            // Normalized checkpoint buffer fill rate on range [cpProgressRate .. 1]. Result value in range [0 .. 1].
            double cpBufFillRateNorm = ((cpBufFillRate - cpProgressRate) / (1d - cpProgressRate));

            return (long)(Math.exp(POW * cpBufFillRateNorm) * MIN_THROTTLE_NANOS);
        }
        else
            return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean reset() {
        return throttlingStarted.getAndSet(false);
    }
}
