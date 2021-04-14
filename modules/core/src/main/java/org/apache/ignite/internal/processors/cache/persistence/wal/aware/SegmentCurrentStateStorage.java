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
class SegmentCurrentStateStorage extends SegmentObservable {
    /** Flag of interrupt of waiting on this object. */
    private volatile boolean interrupted;

    /** Flag of force interrupt of waiting on this object. Needed for uninterrupted waiters. */
    private volatile boolean forceInterrupted;

    /** Total WAL segments count. */
    private final int walSegmentsCnt;

    /**
     * Absolute current segment index WAL Manager writes to. Guarded by <code>this</code>. Incremented during rollover.
     * Also may be directly set if WAL is resuming logging after start.
     */
    private volatile long curAbsWalIdx = -1;

    /** Last archived file absolute index. */
    private volatile long lastAbsArchivedIdx = -1;

    /**
     * Constructor.
     *
     * @param walSegmentsCnt Total WAL segments count.
     */
    SegmentCurrentStateStorage(int walSegmentsCnt) {
        this.walSegmentsCnt = walSegmentsCnt;
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
        //We can archive segment if it less than current work segment so for archivate lastArchiveSegment + 1
        // we should be ensure that currentWorkSegment = lastArchiveSegment + 2
        awaitSegment(lastAbsArchivedIdx + 2);

        return lastAbsArchivedIdx + 1;
    }

    /**
     * Calculate next segment index or wait if needed. Uninterrupted waiting. - for force interrupt used
     * forceInterrupted flag.
     *
     * @return Next absolute segment index.
     */
    long nextAbsoluteSegmentIndex() throws IgniteInterruptedCheckedException {
        long nextAbsIdx;

        synchronized (this) {
            curAbsWalIdx++;

            notifyAll();

            try {
                while (curAbsWalIdx - lastAbsArchivedIdx > walSegmentsCnt && !forceInterrupted)
                    wait();
            }
            catch (InterruptedException e) {
                throw new IgniteInterruptedCheckedException(e);
            }

            if (forceInterrupted)
                throw new IgniteInterruptedCheckedException("Interrupt waiting of change archived idx");

            nextAbsIdx = curAbsWalIdx;
        }

        notifyObservers(nextAbsIdx);

        return nextAbsIdx;
    }

    /**
     * Update current WAL index.
     *
     * @param curAbsWalIdx New current WAL index.
     */
    void curAbsWalIdx(long curAbsWalIdx) {
        synchronized (this) {
            this.curAbsWalIdx = curAbsWalIdx;

            notifyAll();
        }

        notifyObservers(curAbsWalIdx);
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
     *
     * @param lastAbsArchivedIdx Last archived file absolute index.
     */
    synchronized void onSegmentArchived(long lastAbsArchivedIdx) {
        this.lastAbsArchivedIdx = lastAbsArchivedIdx;

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
