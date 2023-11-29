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
            // TODO review CP_BUF_FILL_THRESHOLD_FILL_RATE for checkpoint pages write phase.
            cpBufWatchdog = new CheckpointBufferOverflowWatchdog(pageMemory, CP_BUF_FILL_THRESHOLD_FILL_RATE);
            cpBufProtector = new FillRateBasedThrottlingStrategy(cpBufWatchdog, cpProgress);
        }
        else {
            cpBufWatchdog = new CheckpointBufferOverflowWatchdog(pageMemory, CP_BUF_FILL_THRESHOLD_EXP_BACKOFF);
            cpBufProtector = new ExponentialBackoffThrottlingStrategy();
        }

    }

    /** {@inheritDoc} */
    @Override public boolean isCpBufferOverflowThresholdExceeded() {
        return cpBufWatchdog.isInDangerZone();
    }
}
