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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;

/**
 * Next WAL segment archived monitor. Manages last archived index, allows to emulate archivation in no-archiver mode.
 * Monitor which is notified each time WAL segment is archived.
 */
class SegmentArchivedStorage {
    private final SegmentLockStorage segmentLockStorage;

    /** current thread stopping advice */
    private volatile boolean stopped;
    /**
     * Last archived file absolute index, 0-based. Write is quarded by {@code this}. Negative value indicates there are
     * no segments archived.
     */
    private volatile long lastAbsArchivedIdx = -1;

    List<Consumer<Long>> observers = new ArrayList<>();

    synchronized void addObserver(Consumer<Long> observer) {
        observers.add(observer);
    }

    public SegmentArchivedStorage(
        SegmentLockStorage segmentLockStorage) {
        this.segmentLockStorage = segmentLockStorage;

        segmentLockStorage.addObserver(this::onSegmentUnlocked);
    }

    /**
     * Callback for waking up compressor when new segment is archived.
     */
    synchronized void onSegmentUnlocked(long segmentId) {
        notifyAll();
    }

    /**
     * @return Last archived segment absolute index.
     */
    long lastArchivedAbsoluteIndex() {
        return lastAbsArchivedIdx;
    }

    /**
     * @param lastAbsArchivedIdx new value of last archived segment index
     */
    synchronized void setLastArchivedAbsoluteIndex(long lastAbsArchivedIdx) {
        this.lastAbsArchivedIdx = lastAbsArchivedIdx;

        notifyAll();

        observers.forEach(observer -> observer.accept(lastAbsArchivedIdx));
    }

    /**
     * Method will wait activation of particular WAL segment index.
     *
     * @param awaitIdx absolute index  {@link #lastArchivedAbsoluteIndex()} to become true.
     * @throws IgniteInterruptedCheckedException if interrupted.
     */
    synchronized void awaitSegmentArchived(long awaitIdx) throws IgniteInterruptedCheckedException {
        while (lastArchivedAbsoluteIndex() < awaitIdx) {
            try {
                wait(2000);
            }
            catch (InterruptedException e) {
                throw new IgniteInterruptedCheckedException(e);
            }
        }
    }

    synchronized void markAsMovedToArchive(long toArchive) throws InterruptedException, StopException {
        while (segmentLockStorage.locked(toArchive) && !stopped)
            wait();

        checkForStop();

        // Then increase counter to allow rollover on clean working file
        setLastArchivedAbsoluteIndex(toArchive);
    }


    private void checkForStop() throws StopException {
        if (stopped) {
            throw new StopException();
        }
    }
    /**
     * Stop waiting.
     */
    synchronized void stop() {
        stopped = true;

        notifyAll();
    }
}
