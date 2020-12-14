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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;

/**
 * Storage of actual information about current index of compressed segments.
 */
public class SegmentCompressStorage {
    /** Flag of interrupt waiting on this object. */
    private volatile boolean interrupted;

    /** If WAL compaction enabled. */
    private final boolean compactionEnabled;

    /** Last successfully compressed segment. */
    private volatile long lastCompressedIdx = -1L;

    /** Last enqueued to compress segment. */
    private long lastEnqueuedToCompressIdx = -1L;

    /** Segments to compress queue. */
    private final Queue<Long> segmentsToCompress = new ArrayDeque<>();

    /** List of currently compressing segments. */
    private final List<Long> compressingSegments = new ArrayList<>();

    /** Compressed segment with maximal index. */
    private long lastMaxCompressedIdx = -1L;

    /**
     * Constructor.
     *
     * @param compactionEnabled If WAL compaction enabled.
     */
    SegmentCompressStorage(boolean compactionEnabled) {
        this.compactionEnabled = compactionEnabled;
    }

    /**
     * Callback after segment compression finish.
     *
     * @param compressedIdx Index of compressed segment.
     */
    synchronized void onSegmentCompressed(long compressedIdx) {
        if (compressedIdx > lastMaxCompressedIdx)
            lastMaxCompressedIdx = compressedIdx;

        compressingSegments.remove(compressedIdx);

        if (!compressingSegments.isEmpty())
            this.lastCompressedIdx = Math.min(lastMaxCompressedIdx, compressingSegments.get(0) - 1);
        else
            this.lastCompressedIdx = lastMaxCompressedIdx;

        if (compressedIdx > lastEnqueuedToCompressIdx)
            lastEnqueuedToCompressIdx = compressedIdx;
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
        try {
            while (segmentsToCompress.peek() == null && !interrupted)
                wait();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }

        checkInterrupted();

        Long idx = segmentsToCompress.poll();

        assert idx != null;

        compressingSegments.add(idx);

        return idx;
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
    synchronized void onSegmentArchived(long lastAbsArchivedIdx) {
        while (lastEnqueuedToCompressIdx < lastAbsArchivedIdx && compactionEnabled)
            segmentsToCompress.add(++lastEnqueuedToCompressIdx);

        notifyAll();
    }

    /**
     * Reset interrupted flag.
     */
    public void reset() {
        interrupted = false;
    }
}
