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

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;

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
    private long cpStart = System.currentTimeMillis();

    /** */
    private long cpLockWaitStart;

    /** */
    private long cpMarkStart;

    /** */
    private long cpLockRelease;

    /** */
    private long cpPagesWriteStart;

    /** */
    private long cpFsyncStart;

    /** */
    private long cpEnd;

    /** */
    private long walCpRecordFsyncStart;

    /** */
    private long walCpRecordFsyncEnd;

    /** */
    private long checkpointerLockGet;

    /** */
    private long afterCheckpointListeners;

    /** */
    private long snapshotFutureReady;

    /** */
    private long cacheGroupStatesReady;

    /** */
    private long dirtyPagesCollected;

    /** */
    private long checkpointEntryLogged;

    /**
     * Increments counter if copy on write page was written.
     */
    public void onCowPageWritten() {
        COW_PAGES_UPDATER.incrementAndGet(this);
    }

    /**
     *
     */
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

    /**
     *
     */
    public void onLockWaitStart() {
        cpLockWaitStart = System.currentTimeMillis();
    }

    /**
     *
     */
    public void onMarkStart() {
        cpMarkStart = System.currentTimeMillis();
    }

    /**
     *
     */
    public void onLockRelease() {
        cpLockRelease = System.currentTimeMillis();
    }

    /**
     *
     */
    public void onPagesWriteStart() {
        cpPagesWriteStart = System.currentTimeMillis();
    }

    /**
     *
     */
    public void onFsyncStart() {
        cpFsyncStart = System.currentTimeMillis();
    }

    /**
     *
     */
    public void onEnd() {
        cpEnd = System.currentTimeMillis();
    }

    /**
     *
     */
    public void onWalCpRecordFsyncStart() {
        walCpRecordFsyncStart = System.currentTimeMillis();
    }

    /**
     *
     */
    public void onWalCpRecordFsyncEnd() {
        walCpRecordFsyncEnd = System.currentTimeMillis();
    }

    /**
     * @return Total checkpoint duration.
     */
    public long totalDuration() {
        return cpEnd - cpStart;
    }

    /**
     * @return Checkpoint lock wait duration.
     */
    public long lockWaitDuration() {
        return cpMarkStart - cpLockWaitStart;
    }

    /**
     * @return Checkpoint mark duration.
     */
    public long markDuration() {
        return cpPagesWriteStart - cpMarkStart;
    }

    /**
     * @return Checkpoint lock hold duration.
     */
    public long lockHoldDuration() {
        return cpLockRelease - cpMarkStart;
    }

    /**
     * @return Pages write duration.
     */
    public long pagesWriteDuration() {
        return cpFsyncStart - cpPagesWriteStart;
    }

    /**
     * @return Checkpoint fsync duration.
     */
    public long fsyncDuration() {
        return cpEnd - cpFsyncStart;
    }

    /**
     * @return Duration of WAL fsync after logging {@link CheckpointRecord} on checkpoint begin.
     */
    public long walCpRecordFsyncDuration() {
        return walCpRecordFsyncEnd - walCpRecordFsyncStart;
    }

    /** */
    public void onCheckpointerLockGet() {
        checkpointerLockGet = System.currentTimeMillis();
    }

    /** */
    public long checkpointLockGetDuration() {
        return cpMarkStart - checkpointerLockGet;
    }

    /** */
    public void onAfterCheckpointListeners() {
        afterCheckpointListeners = System.currentTimeMillis();
    }

    /** */
    public long checkpointListenersDuration() {
        return afterCheckpointListeners - checkpointerLockGet;
    }

    /** */
    public void onSnapshotFutureReady() {
        snapshotFutureReady = System.currentTimeMillis();
    }

    /** */
    public long snapshotFutureReadyDuration() {
        return snapshotFutureReady - afterCheckpointListeners;
    }

    /** */
    public void onCacheGroupStatesReady() {
        cacheGroupStatesReady = System.currentTimeMillis();
    }

    /** */
    public long cacheGroupStatesDuration() {
        return cacheGroupStatesReady - snapshotFutureReady;
    }

    /** */
    public void onDirtyPagesCollected() {
        dirtyPagesCollected = System.currentTimeMillis();
    }

    /** */
    public long drtyPagesCollectedDuration() {
        return dirtyPagesCollected - cacheGroupStatesReady;
    }

    /** */
    public void onCheckpointEntryLogged() {
        checkpointEntryLogged = System.currentTimeMillis();
    }

    /** */
    public long checkpointEntryLoggedDuration() {
        return checkpointEntryLogged - dirtyPagesCollected;
    }

    /** */
    private List<DbCheckpointListener.SaveMetadataStat> stats = new ArrayList<>();

    /**
     * @param stat Stat.
     */
    public void addMetaStat(DbCheckpointListener.SaveMetadataStat stat) {
        if (stat == null) {
            System.err.println("Added NULL stat");

            new Exception().printStackTrace();
        }


        stats.add(stat);
    }

    /** */
    public List<DbCheckpointListener.SaveMetadataStat> getStats() {
        return stats;
    }
}
