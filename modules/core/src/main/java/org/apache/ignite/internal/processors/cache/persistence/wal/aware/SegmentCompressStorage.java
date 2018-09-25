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
 * Storage of actual information about current index of compressed segments.
 */
public class SegmentCompressStorage {
    /** Flag of interrupt waiting on this object. */
    private volatile boolean interrupted;
    /** Manages last archived index, emulates archivation in no-archiver mode. */
    private final SegmentArchivedStorage segmentArchivedStorage;
    /** Last successfully compressed segment. */
    private volatile long lastCompressedIdx = -1L;

    /**
     * @param segmentArchivedStorage Storage of last archived segment.
     */
    private SegmentCompressStorage(SegmentArchivedStorage segmentArchivedStorage) {
        this.segmentArchivedStorage = segmentArchivedStorage;

        this.segmentArchivedStorage.addObserver(this::onSegmentArchived);
    }

    /**
     * @param segmentArchivedStorage Storage of last archived segment.
     */
    static SegmentCompressStorage buildCompressStorage(SegmentArchivedStorage segmentArchivedStorage) {
        SegmentCompressStorage storage = new SegmentCompressStorage(segmentArchivedStorage);

        segmentArchivedStorage.addObserver(storage::onSegmentArchived);

        return storage;
    }

    /**
     * Force set last compressed segment.
     *
     * @param lastCompressedIdx Segment which was last compressed.
     */
    void lastCompressedIdx(long lastCompressedIdx) {
        this.lastCompressedIdx = lastCompressedIdx;
    }

    /**
     * @return Last compressed segment.
     */
    long lastCompressedIdx() {
        return lastCompressedIdx;
    }

    /**
     * Pessimistically tries to reserve segment for compression in order to avoid concurrent truncation. Waits if
     * there's no segment to archive right now.
     */
    synchronized long nextSegmentToCompressOrWait() throws IgniteInterruptedCheckedException {
        long segmentToCompress = lastCompressedIdx + 1;

        try {
            while (
                segmentToCompress > segmentArchivedStorage.lastArchivedAbsoluteIndex()
                    && !interrupted
                )
                wait();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }

        checkInterrupted();

        return segmentToCompress;
    }

    /**
     * Interrupt waiting on this object.
     */
    synchronized void interrupt() {
        interrupted = true;

        notifyAll();
    }

    /**
     * Check for interrupt flag was set.
     */
    private void checkInterrupted() throws IgniteInterruptedCheckedException {
        if (interrupted)
            throw new IgniteInterruptedCheckedException("Interrupt waiting of change compressed idx");
    }

    /**
     * Callback for waking up compressor when new segment is archived.
     */
    private synchronized void onSegmentArchived(long lastAbsArchivedIdx) {
        notifyAll();
    }

}
