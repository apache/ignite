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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.lang.IgniteOutClosure;

/**
 * Abstract throttling policy
 */
public abstract class AbstractPagesWriteThrottle implements PagesWriteThrottlePolicy {
    /** Page memory. */
    protected final PageMemoryImpl pageMemory;

    /** Checkpoint progress provider. */
    protected final IgniteOutClosure<CheckpointProgress> cpProgress;

    /** Checkpoint lock state checker. */
    protected final CheckpointLockStateChecker cpLockStateChecker;

    /** Checkpoint buffer protection logic. */
    protected final ThrottlingStrategy cpBufProtector;

    /** Checkpoint Buffer-related logic used to keep it safe. */
    protected final CheckpointBufferOverflowWatchdog cpBufWatchdog;

    /** Logger. */
    protected final IgniteLogger log;

    /**
     * @param pageMemory Page memory.
     * @param cpProgress Checkpoint progress provider.
     * @param cpLockStateChecker checkpoint lock state checker.
     * @param fillRateBasedCpBufProtection If true, fill rate based throttling will be used to protect from
     *        checkpoint buffer overflow.
     * @param log Logger.
     */
    protected AbstractPagesWriteThrottle(
        PageMemoryImpl pageMemory,
        IgniteOutClosure<CheckpointProgress> cpProgress,
        CheckpointLockStateChecker cpLockStateChecker,
        boolean fillRateBasedCpBufProtection,
        IgniteLogger log
    ) {
        this.pageMemory = pageMemory;
        this.cpProgress = cpProgress;
        this.cpLockStateChecker = cpLockStateChecker;
        this.log = log;

        if (fillRateBasedCpBufProtection) {
            cpBufWatchdog = new CheckpointBufferOverflowWatchdog(pageMemory, CP_BUF_THROTTLING_THRESHOLD_FILL_RATE,
                CP_BUF_DANGER_THRESHOLD, CP_BUF_WAKEUP_THRESHOLD_FILL_RATE);
            cpBufProtector = new FillRateBasedThrottlingStrategy(cpBufWatchdog, cpProgress);
        }
        else {
            cpBufWatchdog = new CheckpointBufferOverflowWatchdog(pageMemory, CP_BUF_DANGER_THRESHOLD,
                CP_BUF_DANGER_THRESHOLD, CP_BUF_WAKEUP_THRESHOLD_EXP_BACKOFF);
            cpBufProtector = new ExponentialBackoffThrottlingStrategy();
        }

    }

    /** */
    @Override public int checkpointBufferThrottledThreadsWakeupThreshold() {
        return cpBufWatchdog.checkpointBufferThrottledThreadsWakeupThreshold();
    }

    /** {@inheritDoc} */
    @Override public boolean isCpBufferOverflowThresholdExceeded() {
        return cpBufWatchdog.isInDangerZone();
    }
}
