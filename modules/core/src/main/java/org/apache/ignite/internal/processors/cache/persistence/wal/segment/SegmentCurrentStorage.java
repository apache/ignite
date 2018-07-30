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

package org.apache.ignite.internal.processors.cache.persistence.wal.segment;

/**
 * Storage for state of absolute current segment index.
 */
class SegmentCurrentStorage {
    /** current thread stopping advice */
    private volatile boolean stopped;

    /** WAL segments count. */
    private final int walSegmentsCount;

    private final SegmentArchivedStorage archivedMonitor;
    /**
     * Absolute current segment index WAL Manager writes to. Guarded by <code>this</code>. Incremented during rollover.
     * Also may be directly set if WAL is resuming logging after start.
     */
    private long curAbsWalIdx = -1;

    /**
     * @param count WAL segments count.
     * @param monitor Last archived segment storage.
     */
    public SegmentCurrentStorage(int count, SegmentArchivedStorage monitor) {
        walSegmentsCount = count;
        archivedMonitor = monitor;

        archivedMonitor.addObserver(this::onSegmentArchived);
    }

    /**
     * Waiting until current WAL index will be greater than given one.
     *
     * @param absSegIdx Target WAL index.
     */
    public void awaitSegment(long absSegIdx) throws InterruptedException, StopException {
        synchronized (this) {
            while (curAbsWalIdx < absSegIdx && !stopped)
                wait();

            checkForStop();

            // If the archive directory is empty, we can be sure that there were no WAL segments archived.
            // This is ensured by the check in truncate() which will leave at least one file there
            // once it was archived.
        }
    }

    private void checkForStop() throws StopException {
        if (stopped) {
            throw new StopException();
        }
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
     * Calculate next segment index or wait if needed.
     *
     * @return Next absolute segment index.
     */
    synchronized long nextAbsoluteSegmentIndex() throws InterruptedException, StopException {
        curAbsWalIdx++;

        // Notify archiver thread.
        notifyAll();

        while (curAbsWalIdx - archivedMonitor.lastArchivedAbsoluteIndex() > walSegmentsCount && !stopped)
            wait();

        checkForStop();

        return curAbsWalIdx;
    }

    /**
     * Callback for waking up awaiting when new segment is archived.
     */
    private synchronized void onSegmentArchived(long lastAbsArchivedIdx) {
        notifyAll();
    }

    long curAbsWalIdx() {
        return curAbsWalIdx;
    }

    /**
     * Stop waiting.
     */
    synchronized void stop() {
        stopped = true;

        notifyAll();
    }
}
