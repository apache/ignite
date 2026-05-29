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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntryType;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointMarkersStorage;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;

/**
 * Tracks various checkpoint phases and stats.
 *
 * Assumed sequence of events:
 * <ol>
 *     <li>Checkpoint start</li>
 *     <li>CP Lock wait start</li>
 *     <li>CP mark start</li>
 *     <li>CP Lock release</li>
 *     <li>Pages write start</li>
 *     <li>fsync start</li>
 *     <li>Checkpoint end</li>
 * </ol>
 */
public class CheckpointMetricsTracker {
    /** */
    private static final AtomicIntegerFieldUpdater<CheckpointMetricsTracker> DATA_PAGES_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(CheckpointMetricsTracker.class, "dataPages");

    /** */
    private static final AtomicIntegerFieldUpdater<CheckpointMetricsTracker> COW_PAGES_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(CheckpointMetricsTracker.class, "cowPages");

    /** */
    private volatile int dataPages;

    /** */
    private volatile int cowPages;

    /** */
    private final long cpStart = System.nanoTime();

    /** */
    private long cpEnd;

    /** Epoch time of checkpoint process start */
    private final long cpStartEpochTime = System.currentTimeMillis();

    /** */
    private long cpLockWaitStart;

    /** */
    private long cpMarkStart;

    /** */
    private long cpMarkEnd;

    /** */
    private long cpPagesWriteStart;

    /** */
    private long cpFsyncStart;

    /** */
    private long walCpRecordFsyncStart;

    /** */
    private long walCpRecordFsyncEnd;

    /** */
    private long cpRecoveryDataWriteEnd;

    /** */
    private long cpRecoveryDataSize;

    /** */
    private long totalDuration;

    /** */
    private long totalDurationNanos;

    /** */
    private long fsyncDuration;

    /** */
    private long writeCheckpointEntryDuration;

    /** */
    private long recoveryDataWriteDuration;

    /** */
    private long splitAndSortCpPagesDuration;

    /** */
    private long walCpRecordFsyncDuration;

    /** */
    private long pagesWriteDuration;

    /** */
    private long lockDurationMillis;

    /** */
    private long lockHoldDuration;

    /** */
    private long markDuration;

    /** */
    private long listenersExecDuration;

    /** */
    private long beforeLockDuration;

    /** */
    private long lockWaitDuration;

    /**
     * Increments counter if copy on write page was written.
     */
    public void onCowPageWritten() {
        COW_PAGES_UPDATER.incrementAndGet(this);
    }

    /** */
    public void onDataPageWritten() {
        DATA_PAGES_UPDATER.incrementAndGet(this);
    }

    /**
     * @return COW pages.
     */
    public int cowPagesWritten() {
        return cowPages;
    }

    /**
     * @return Data pages written.
     */
    public int dataPagesWritten() {
        return dataPages;
    }

    /** */
    public void onLockWaitStart() {
        cpLockWaitStart = System.nanoTime();
        beforeLockDuration = TimeUnit.NANOSECONDS.toMillis(cpLockWaitStart - cpStart);
    }

    /** */
    public void onMarkStart() {
        cpMarkStart = System.nanoTime();
        lockWaitDuration = TimeUnit.NANOSECONDS.toMillis(cpMarkStart - cpLockWaitStart);
    }

    /** */
    public void onMarkEnd() {
        cpMarkEnd = System.nanoTime();
        markDuration = TimeUnit.NANOSECONDS.toMillis(cpMarkEnd - cpMarkStart);
    }

    /** */
    public void onLockRelease() {
        long nanoTime = System.nanoTime();
        lockHoldDuration = TimeUnit.NANOSECONDS.toMillis(nanoTime - cpMarkStart);
        lockDurationMillis = TimeUnit.NANOSECONDS.toMillis(nanoTime - cpLockWaitStart);
    }

    /** */
    public void onPagesWriteStart() {
        cpPagesWriteStart = System.nanoTime();
    }

    /** */
    public void onFsyncStart() {
        cpFsyncStart = System.nanoTime();
        pagesWriteDuration = TimeUnit.NANOSECONDS.toMillis(cpFsyncStart - cpPagesWriteStart);
    }

    /** */
    public void onEnd() {
        cpEnd = System.nanoTime();
        totalDurationNanos = cpEnd - cpStart;
        totalDuration = TimeUnit.NANOSECONDS.toMillis(totalDurationNanos);
        fsyncDuration = TimeUnit.NANOSECONDS.toMillis(cpEnd - cpFsyncStart);
    }

    /** */
    public void onListenersExecuteEnd() {
        listenersExecDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - cpMarkStart);
    }

    /** */
    public void onWalCpRecordFsyncStart() {
        walCpRecordFsyncStart = System.nanoTime();
    }

    /** */
    public void onCpMarkerStoreEnd() {
        writeCheckpointEntryDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - cpRecoveryDataWriteEnd);
    }

    /** */
    public void onSplitAndSortCpPagesEnd() {
        splitAndSortCpPagesDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - walCpRecordFsyncEnd);
    }

    /** */
    public void onWalCpRecordFsyncEnd() {
        walCpRecordFsyncEnd = System.nanoTime();
        walCpRecordFsyncDuration = TimeUnit.NANOSECONDS.toMillis(walCpRecordFsyncEnd - walCpRecordFsyncStart);
    }

    /** */
    public void onWriteRecoveryDataEnd(long recoveryDataSize) {
        cpRecoveryDataSize = recoveryDataSize;
        cpRecoveryDataWriteEnd = System.nanoTime();
        recoveryDataWriteDuration = TimeUnit.NANOSECONDS.toMillis(cpRecoveryDataWriteEnd - cpMarkEnd);
    }

    /**
     * @return Total checkpoint duration.
     */
    public long totalDuration() {
        return totalDuration;
    }

    /**
     * @return Total checkpoint duration (nanos).
     */
    public long totalDurationNanos() {
        return totalDurationNanos;
    }

    /**
     * @return Checkpoint lock wait duration.
     */
    public long lockWaitDuration() {
        return lockWaitDuration;
    }

    /**
     * @return Checkpoint action before taken write lock duration.
     */
    public long beforeLockDuration() {
        return beforeLockDuration;
    }

    /**
     * @return Execution listeners under write lock duration.
     */
    public long listenersExecuteDuration() {
        return listenersExecDuration;
    }

    /**
     * @return Checkpoint mark duration.
     */
    public long markDuration() {
        return markDuration;
    }

    /**
     * @return Checkpoint lock hold duration.
     */
    public long lockHoldDuration() {
        return lockHoldDuration;
    }

    /**
     * @return Checkpoint lock duration
     */
    public long lockDurationMillis() {
        return lockDurationMillis;
    }

    /**
     * @return Pages write duration.
     */
    public long pagesWriteDuration() {
        return pagesWriteDuration;
    }

    /**
     * @return Checkpoint fsync duration.
     */
    public long fsyncDuration() {
        return fsyncDuration;
    }

    /**
     * @return Duration of WAL fsync after logging {@link CheckpointRecord} on checkpoint begin.
     */
    public long walCpRecordFsyncDuration() {
        return walCpRecordFsyncDuration;
    }

    /**
     * @return Duration of splitting and sorting checkpoint pages.
     */
    public long splitAndSortCpPagesDuration() {
        return splitAndSortCpPagesDuration;
    }

    /**
     * @return Duration of writing recovery data.
     */
    public long recoveryDataWriteDuration() {
        return recoveryDataWriteDuration;
    }

    /**
     * @return Size of writing recovery data.
     */
    public long recoveryDataSize() {
        return cpRecoveryDataSize;
    }

    /**
     * @return Duration of checkpoint entry buffer writing to file.
     *
     * @see CheckpointMarkersStorage#writeCheckpointEntry(long, UUID, WALPointer, CheckpointRecord, CheckpointEntryType, boolean)
     */
    public long writeCheckpointEntryDuration() {
        return writeCheckpointEntryDuration;
    }

    /**
     * @return Checkpoint start time.
     */
    public long checkpointStartTime() {
        return cpStartEpochTime;
    }

    /**
     * @return monotonic time in nanos at check point process start
     */
    public long checkPointStartNanos() {
        return cpStart;
    }

    /**
     * @return monotonic time in nanos at check point process end
     */
    public long checkPointEndNanos() {
        return cpEnd;
    }
}
