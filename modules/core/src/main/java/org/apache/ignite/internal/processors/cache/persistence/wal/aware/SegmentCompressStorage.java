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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Storage of actual information about current index of compressed segments.
 */
public class SegmentCompressStorage {
    /** Flag of interrupt waiting on this object. */
    private volatile boolean interrupted;

    /** Manages last archived index, emulates archivation in no-archiver mode. */
    private final SegmentArchivedStorage segmentArchivedStorage;

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

    /** Min uncompressed index to keep. */
    private volatile long minUncompressedIdxToKeep = -1L;

    /**
     * @param segmentArchivedStorage Storage of last archived segment.
     * @param compactionEnabled If WAL compaction enabled.
     */
    private SegmentCompressStorage(SegmentArchivedStorage segmentArchivedStorage, boolean compactionEnabled) {
        this.segmentArchivedStorage = segmentArchivedStorage;

        this.compactionEnabled = compactionEnabled;

        this.segmentArchivedStorage.addObserver(this::onSegmentArchived);
    }

    /**
     * @param segmentArchivedStorage Storage of last archived segment.
     * @param compactionEnabled If WAL compaction enabled.
     */
    static SegmentCompressStorage buildCompressStorage(SegmentArchivedStorage segmentArchivedStorage,
                                                       boolean compactionEnabled) {
        SegmentCompressStorage storage = new SegmentCompressStorage(segmentArchivedStorage, compactionEnabled);

        segmentArchivedStorage.addObserver(storage::onSegmentArchived);

        return storage;
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
    private synchronized void onSegmentArchived(long lastAbsArchivedIdx) {
        while (lastEnqueuedToCompressIdx < lastAbsArchivedIdx && compactionEnabled)
            segmentsToCompress.add(++lastEnqueuedToCompressIdx);

        notifyAll();
    }

    /**
     * @param idx Minimum raw segment index that should be preserved from deletion.
     */
    void keepUncompressedIdxFrom(long idx) {
        minUncompressedIdxToKeep = idx;
    }

    /**
     * @return  Minimum raw segment index that should be preserved from deletion.
     */
    long keepUncompressedIdxFrom() {
        return minUncompressedIdxToKeep;
    }

    /**
     * Reset interrupted flag.
     */
    public void reset() {
        interrupted = false;
    }
}
