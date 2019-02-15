/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class DataStorageMetricsSnapshot implements DataStorageMetrics {
    /** */
    private float walLoggingRate;

    /** */
    private float walWritingRate;

    /** */
    private int walArchiveSegments;

    /** */
    private float walFsyncTimeAvg;

    /** */
    private long walBuffPollSpinsNum;

    /** */
    private long lastCpDuration;

    /** */
    private long lastCpLockWaitDuration;

    /** */
    private long lastCpMmarkDuration;

    /** */
    private long lastCpPagesWriteDuration;

    /** */
    private long lastCpFsyncDuration;

    /** */
    private long lastCpTotalPages;

    /** */
    private long lastCpDataPages;

    /** */
    private long lastCpCowPages;

    /** */
    private long walTotalSize;

    /** */
    private long walLastRollOverTime;

    /** */
    private long checkpointTotalTime;

    /** */
    private long usedCheckpointBufferSize;

    /** */
    private long usedCheckpointBufferPages;

    /** */
    private long checkpointBufferSize;

    /** */
    private long dirtyPages;

    /** */
    private long readPages;

    /** */
    private long writtenPages;

    /** */
    private long replacedPages;

    /** */
    private long offHeapSize;

    /** */
    private long offHeadUsedSize;

    /** */
    private long totalAllocatedSize;

    /**
     * @param metrics Metrics.
     */
    public DataStorageMetricsSnapshot(DataStorageMetrics metrics) {
        walLoggingRate = metrics.getWalLoggingRate();
        walWritingRate = metrics.getWalWritingRate();
        walArchiveSegments = metrics.getWalArchiveSegments();
        walFsyncTimeAvg = metrics.getWalFsyncTimeAverage();
        walBuffPollSpinsNum = metrics.getWalBuffPollSpinsRate();
        lastCpDuration = metrics.getLastCheckpointDuration();
        lastCpLockWaitDuration = metrics.getLastCheckpointLockWaitDuration();
        lastCpMmarkDuration = metrics.getLastCheckpointMarkDuration();
        lastCpPagesWriteDuration = metrics.getLastCheckpointPagesWriteDuration();
        lastCpFsyncDuration = metrics.getLastCheckpointFsyncDuration();
        lastCpTotalPages = metrics.getLastCheckpointTotalPagesNumber();
        lastCpDataPages = metrics.getLastCheckpointDataPagesNumber();
        lastCpCowPages = metrics.getLastCheckpointCopiedOnWritePagesNumber();
        walTotalSize = metrics.getWalTotalSize();
        walLastRollOverTime = metrics.getWalLastRollOverTime();
        checkpointTotalTime = metrics.getCheckpointTotalTime();
        usedCheckpointBufferSize = metrics.getUsedCheckpointBufferSize();
        usedCheckpointBufferPages = metrics.getUsedCheckpointBufferPages();
        checkpointBufferSize = metrics.getCheckpointBufferSize();
        dirtyPages = metrics.getDirtyPages();
        readPages = metrics.getPagesRead();
        writtenPages = metrics.getPagesWritten();
        replacedPages = metrics.getPagesReplaced();
        offHeapSize = metrics.getOffHeapSize();
        offHeadUsedSize = metrics.getOffheapUsedSize();
        totalAllocatedSize = metrics.getTotalAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public float getWalLoggingRate() {
        return walLoggingRate;
    }

    /** {@inheritDoc} */
    @Override public float getWalWritingRate() {
        return walWritingRate;
    }

    /** {@inheritDoc} */
    @Override public int getWalArchiveSegments() {
        return walArchiveSegments;
    }

    /** {@inheritDoc} */
    @Override public float getWalFsyncTimeAverage() {
        return walFsyncTimeAvg;
    }

    /** {@inheritDoc} */
    @Override public long getWalBuffPollSpinsRate() {
        return walBuffPollSpinsNum;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointDuration() {
        return lastCpDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointLockWaitDuration() {
        return lastCpLockWaitDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointMarkDuration() {
        return lastCpMmarkDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointPagesWriteDuration() {
        return lastCpPagesWriteDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointFsyncDuration() {
        return lastCpFsyncDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointTotalPagesNumber() {
        return lastCpTotalPages;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointDataPagesNumber() {
        return lastCpDataPages;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointCopiedOnWritePagesNumber() {
        return lastCpCowPages;
    }

    /** {@inheritDoc} */
    @Override public long getWalTotalSize() {
        return walTotalSize;
    }

    /** {@inheritDoc} */
    @Override public long getWalLastRollOverTime() {
        return walLastRollOverTime;
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointTotalTime() {
        return checkpointTotalTime;
    }

    /** {@inheritDoc} */
    @Override public long getDirtyPages() {
        return dirtyPages;
    }

    /** {@inheritDoc} */
    @Override public long getPagesRead() {
        return readPages;
    }

    /** {@inheritDoc} */
    @Override public long getPagesWritten() {
        return writtenPages;
    }

    /** {@inheritDoc} */
    @Override public long getPagesReplaced() {
        return replacedPages;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapSize() {
        return offHeapSize;
    }

    /** {@inheritDoc} */
    @Override public long getOffheapUsedSize() {
        return offHeadUsedSize;
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedSize() {
        return totalAllocatedSize;
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferPages() {
        return usedCheckpointBufferPages;
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferSize() {
        return usedCheckpointBufferSize;
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointBufferSize(){
        return checkpointBufferSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStorageMetricsSnapshot.class, this);
    }
}
