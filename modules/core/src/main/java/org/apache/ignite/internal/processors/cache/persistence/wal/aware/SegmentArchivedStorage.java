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
package org.apache.ignite.internal.processors.cache.persistence.wal.aware;

import org.apache.ignite.internal.IgniteInterruptedCheckedException;

/**
 * Manages last archived index, allows to emulate archivation in no-archiver mode. Monitor which is notified each time
 * WAL segment is archived.
 *
 * Class for inner usage.
 */
class SegmentArchivedStorage extends SegmentObservable {
    /** Segment lock storage: Protects WAL work segments from moving. */
    private final SegmentLockStorage segmentLockStorage;
    /** Flag of interrupt waiting on this object. */
    private volatile boolean interrupted;
    /**
     * Last archived file absolute index, 0-based. Write is quarded by {@code this}. Negative value indicates there are
     * no segments archived.
     */
    private volatile long lastAbsArchivedIdx = -1;
    /** Latest truncated segment. */
    private volatile long lastTruncatedArchiveIdx = -1;

    /**
     * @param segmentLockStorage Protects WAL work segments from moving.
     */
    private SegmentArchivedStorage(SegmentLockStorage segmentLockStorage) {
        this.segmentLockStorage = segmentLockStorage;
    }

    /**
     * @param segmentLockStorage Protects WAL work segments from moving.
     */
    static SegmentArchivedStorage buildArchivedStorage(SegmentLockStorage segmentLockStorage) {
        SegmentArchivedStorage archivedStorage = new SegmentArchivedStorage(segmentLockStorage);

        segmentLockStorage.addObserver(archivedStorage::onSegmentUnlocked);

        return archivedStorage;
    }

    /**
     * @return Last archived segment absolute index.
     */
    long lastArchivedAbsoluteIndex() {
        return lastAbsArchivedIdx;
    }

    /**
     * @param lastAbsArchivedIdx New value of last archived segment index.
     */
    void setLastArchivedAbsoluteIndex(long lastAbsArchivedIdx) {
        synchronized (this) {
            this.lastAbsArchivedIdx = lastAbsArchivedIdx;

            notifyAll();
        }

        notifyObservers(lastAbsArchivedIdx);
    }

    /**
     * Method will wait activation of particular WAL segment index.
     *
     * @param awaitIdx absolute index  {@link #lastArchivedAbsoluteIndex()} to become true.
     * @throws IgniteInterruptedCheckedException if interrupted.
     */
    synchronized void awaitSegmentArchived(long awaitIdx) throws IgniteInterruptedCheckedException {
        while (lastArchivedAbsoluteIndex() < awaitIdx && !interrupted) {
            try {
                wait(2000);
            }
            catch (InterruptedException e) {
                throw new IgniteInterruptedCheckedException(e);
            }
        }

        checkInterrupted();
    }

    /**
     * Mark segment as moved to archive under lock.
     *
     * @param toArchive Segment which was should be moved to archive.
     * @throws IgniteInterruptedCheckedException if interrupted during waiting.
     */
    synchronized void markAsMovedToArchive(long toArchive) throws IgniteInterruptedCheckedException {
        try {
            while (segmentLockStorage.locked(toArchive) && !interrupted)
                wait();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }

        //Ignore interrupted flag and force set new value. - legacy logic.
        //checkInterrupted();

        setLastArchivedAbsoluteIndex(toArchive);
    }

    /**
     * Interrupt waiting on this object.
     */
    synchronized void interrupt() {
        interrupted = true;

        notifyAll();
    }

    /**
     * Resets interrupted flag.
     */
    void reset() {
        interrupted = false;
    }

    /**
     * Check for interrupt flag was set.
     */
    private void checkInterrupted() throws IgniteInterruptedCheckedException {
        if (interrupted)
            throw new IgniteInterruptedCheckedException("Interrupt waiting of change archived idx");
    }

    /**
     * Callback for waking up waiters of this object when unlocked happened.
     */
    private synchronized void onSegmentUnlocked(long segmentId) {
        notifyAll();
    }

    /**
     * @param lastTruncatedArchiveIdx Last truncated segment.
     */
    void lastTruncatedArchiveIdx(long lastTruncatedArchiveIdx) {
        this.lastTruncatedArchiveIdx = lastTruncatedArchiveIdx;
    }

    /**
     * @return Last truncated segment.
     */
    long lastTruncatedArchiveIdx() {
        return lastTruncatedArchiveIdx;
    }
}
