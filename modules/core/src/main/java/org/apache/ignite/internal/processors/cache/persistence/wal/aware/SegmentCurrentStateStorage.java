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

package org.apache.ignite.internal.processors.cache.persistence.wal.aware;

import org.apache.ignite.internal.IgniteInterruptedCheckedException;

/**
 * Storage of absolute current segment index.
 */
class SegmentCurrentStateStorage {
    /** Flag of interrupt of waiting on this object. */
    private volatile boolean interrupted;

    /** Flag of force interrupt of waiting on this object. Needed for uninterrupted waiters. */
    private volatile boolean forceInterrupted;

    /** Total WAL segments count. */
    private final int walSegmentsCnt;

    /** Manages last archived index, emulates archivation in no-archiver mode. */
    private final SegmentArchivedStorage segmentArchivedStorage;

    /**
     * Absolute current segment index WAL Manager writes to. Guarded by <code>this</code>. Incremented during rollover.
     * Also may be directly set if WAL is resuming logging after start.
     */
    private volatile long curAbsWalIdx = -1;

    /**
     * @param walSegmentsCnt Total WAL segments count.
     * @param segmentArchivedStorage Last archived segment storage.
     */
    private SegmentCurrentStateStorage(int walSegmentsCnt, SegmentArchivedStorage segmentArchivedStorage) {
        this.walSegmentsCnt = walSegmentsCnt;
        this.segmentArchivedStorage = segmentArchivedStorage;
    }

    /**
     * @param walSegmentsCnt Total WAL segments count.
     * @param segmentArchivedStorage Last archived segment storage.
     */
    static SegmentCurrentStateStorage buildCurrentStateStorage(
        int walSegmentsCnt,
        SegmentArchivedStorage segmentArchivedStorage
    ) {

        SegmentCurrentStateStorage currStorage = new SegmentCurrentStateStorage(walSegmentsCnt, segmentArchivedStorage);

        segmentArchivedStorage.addObserver(currStorage::onSegmentArchived);

        return currStorage;
    }

    /**
     * Waiting until current WAL index will be greater or equal than given one.
     *
     * @param absSegIdx Target WAL index.
     */
    synchronized void awaitSegment(long absSegIdx) throws IgniteInterruptedCheckedException {
        try {
            while (curAbsWalIdx < absSegIdx && !interrupted)
                wait();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }

        checkInterrupted();
    }

    /**
     * Waiting until archivation of next segment will be allowed.
     */
    synchronized long waitNextSegmentForArchivation() throws IgniteInterruptedCheckedException {
        long lastArchivedSegment = segmentArchivedStorage.lastArchivedAbsoluteIndex();

        //We can archive segment if it less than current work segment so for archivate lastArchiveSegment + 1
        // we should be ensure that currentWorkSegment = lastArchiveSegment + 2
        awaitSegment(lastArchivedSegment + 2);

        return lastArchivedSegment + 1;
    }

    /**
     * Calculate next segment index or wait if needed. Uninterrupted waiting. - for force interrupt used
     * forceInterrupted flag.
     *
     * @return Next absolute segment index.
     */
    synchronized long nextAbsoluteSegmentIndex() throws IgniteInterruptedCheckedException {
        curAbsWalIdx++;

        notifyAll();

        try {
            while (curAbsWalIdx - segmentArchivedStorage.lastArchivedAbsoluteIndex() > walSegmentsCnt && !forceInterrupted)
                wait();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }

        if (forceInterrupted)
            throw new IgniteInterruptedCheckedException("Interrupt waiting of change archived idx");

        return curAbsWalIdx;
    }

    /**
     * Update current WAL index.
     *
     * @param curAbsWalIdx New current WAL index.
     */
    synchronized void curAbsWalIdx(long curAbsWalIdx) {
        this.curAbsWalIdx = curAbsWalIdx;

        notifyAll();
    }

    /**
     * @return Current WAL index.
     */
    long curAbsWalIdx() {
        return this.curAbsWalIdx;
    }

    /**
     * Interrupt waiting on this object.
     */
    synchronized void interrupt() {
        interrupted = true;

        notifyAll();
    }

    /**
     * Interrupt waiting on this object.
     */
    synchronized void forceInterrupt() {
        interrupted = true;
        forceInterrupted = true;

        notifyAll();
    }

    /**
     * Callback for waking up awaiting when new segment is archived.
     */
    private synchronized void onSegmentArchived(long lastAbsArchivedIdx) {
        notifyAll();
    }

    /**
     * Check for interrupt flag was set.
     */
    private void checkInterrupted() throws IgniteInterruptedCheckedException {
        if (interrupted)
            throw new IgniteInterruptedCheckedException("Interrupt waiting of change current idx");
    }

    /**
     * Reset interrupted flag.
     */
    public void reset() {
        interrupted = false;

        forceInterrupted = false;
    }
}
