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

import static org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentArchivedStorage.buildArchivedStorage;
import static org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentCompressStorage.buildCompressStorage;
import static org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentCurrentStateStorage.buildCurrentStateStorage;

/**
 * Holder of actual information of latest manipulation on WAL segments.
 */
public class SegmentAware {
    /** Latest truncated segment. */
    private volatile long lastTruncatedArchiveIdx = -1L;
    /** Segment reservations storage: Protects WAL segments from deletion during WAL log cleanup. */
    private final SegmentReservationStorage reservationStorage = new SegmentReservationStorage();
    /** Lock on segment protects from archiving segment. */
    private final SegmentLockStorage segmentLockStorage = new SegmentLockStorage();
    /** Manages last archived index, emulates archivation in no-archiver mode. */
    private final SegmentArchivedStorage segmentArchivedStorage = buildArchivedStorage(segmentLockStorage);
    /** Storage of actual information about current index of compressed segments. */
    private final SegmentCompressStorage segmentCompressStorage = buildCompressStorage(segmentArchivedStorage);
    /** Storage of absolute current segment index. */
    private final SegmentCurrentStateStorage segmentCurrStateStorage;

    /**
     * @param walSegmentsCnt Total WAL segments count.
     */
    public SegmentAware(int walSegmentsCnt) {
        segmentCurrStateStorage = buildCurrentStateStorage(walSegmentsCnt, segmentArchivedStorage);
    }

    /**
     * Waiting until current WAL index will be greater or equal than given one.
     *
     * @param absSegIdx Target WAL index.
     */
    public void awaitSegment(long absSegIdx) throws IgniteInterruptedCheckedException {
        segmentCurrStateStorage.awaitSegment(absSegIdx);
    }

    /**
     * Calculate next segment index or wait if needed.
     *
     * @return Next absolute segment index.
     */
    public long nextAbsoluteSegmentIndex() throws IgniteInterruptedCheckedException {
        return segmentCurrStateStorage.nextAbsoluteSegmentIndex();
    }

    /**
     * @return Current WAL index.
     */
    public long curAbsWalIdx() {
        return segmentCurrStateStorage.curAbsWalIdx();
    }

    /**
     * Waiting until archivation of next segment will be allowed.
     */
    public long waitNextSegmentForArchivation() throws IgniteInterruptedCheckedException {
        return segmentCurrStateStorage.waitNextSegmentForArchivation();
    }

    /**
     * Mark segment as moved to archive under lock.
     *
     * @param toArchive Segment which was should be moved to archive.
     * @throws IgniteInterruptedCheckedException if interrupted during waiting.
     */
    public void markAsMovedToArchive(long toArchive) throws IgniteInterruptedCheckedException {
        segmentArchivedStorage.markAsMovedToArchive(toArchive);
    }

    /**
     * Method will wait activation of particular WAL segment index.
     *
     * @param awaitIdx absolute index  {@link #lastArchivedAbsoluteIndex()} to become true.
     * @throws IgniteInterruptedCheckedException if interrupted.
     */
    public void awaitSegmentArchived(long awaitIdx) throws IgniteInterruptedCheckedException {
        segmentArchivedStorage.awaitSegmentArchived(awaitIdx);
    }

    /**
     * Pessimistically tries to reserve segment for compression in order to avoid concurrent truncation. Waits if
     * there's no segment to archive right now.
     */
    public long waitNextSegmentToCompress() throws IgniteInterruptedCheckedException {
        return Math.max(segmentCompressStorage.nextSegmentToCompressOrWait(), lastTruncatedArchiveIdx + 1);
    }

    /**
     * Force set last compressed segment.
     *
     * @param lastCompressedIdx Segment which was last compressed.
     */
    public void lastCompressedIdx(long lastCompressedIdx) {
        segmentCompressStorage.lastCompressedIdx(lastCompressedIdx);
    }

    /**
     * @return Last compressed segment.
     */
    public long lastCompressedIdx() {
        return segmentCompressStorage.lastCompressedIdx();
    }

    /**
     * Update current WAL index.
     *
     * @param curAbsWalIdx New current WAL index.
     */
    public void curAbsWalIdx(long curAbsWalIdx) {
        segmentCurrStateStorage.curAbsWalIdx(curAbsWalIdx);
    }

    /**
     * @param lastTruncatedArchiveIdx Last truncated segment;
     */
    public void lastTruncatedArchiveIdx(long lastTruncatedArchiveIdx) {
        this.lastTruncatedArchiveIdx = lastTruncatedArchiveIdx;
    }

    /**
     * @return Last truncated segment.
     */
    public long lastTruncatedArchiveIdx() {
        return lastTruncatedArchiveIdx;
    }

    /**
     * @param lastAbsArchivedIdx New value of last archived segment index.
     */
    public void setLastArchivedAbsoluteIndex(long lastAbsArchivedIdx) {
        segmentArchivedStorage.setLastArchivedAbsoluteIndex(lastAbsArchivedIdx);
    }

    /**
     * @return Last archived segment absolute index.
     */
    public long lastArchivedAbsoluteIndex() {
        return segmentArchivedStorage.lastArchivedAbsoluteIndex();
    }

    /**
     * @param absIdx Index for reservation.
     */
    public void reserve(long absIdx) {
        reservationStorage.reserve(absIdx);
    }

    /**
     * Checks if segment is currently reserved (protected from deletion during WAL cleanup).
     *
     * @param absIdx Index for check reservation.
     * @return {@code True} if index is reserved.
     */
    public boolean reserved(long absIdx) {
        return reservationStorage.reserved(absIdx);
    }

    /**
     * @param absIdx Reserved index.
     */
    public void release(long absIdx) {
        reservationStorage.release(absIdx);
    }

    /**
     * Check if WAL segment locked (protected from move to archive)
     *
     * @param absIdx Index for check reservation.
     * @return {@code True} if index is locked.
     */
    public boolean locked(long absIdx) {
        return segmentLockStorage.locked(absIdx);
    }

    /**
     * @param absIdx Segment absolute index.
     * @return <ul><li>{@code True} if can read, no lock is held, </li><li>{@code false} if work segment, need release
     * segment later, use {@link #releaseWorkSegment} for unlock</li> </ul>
     */
    public boolean checkCanReadArchiveOrReserveWorkSegment(long absIdx) {
        return lastArchivedAbsoluteIndex() >= absIdx || segmentLockStorage.lockWorkSegment(absIdx);
    }

    /**
     * @param absIdx Segment absolute index.
     */
    public void releaseWorkSegment(long absIdx) {
        segmentLockStorage.releaseWorkSegment(absIdx);
    }

    /**
     * Interrupt waiting on related objects.
     */
    public void interrupt() {
        segmentArchivedStorage.interrupt();

        segmentCompressStorage.interrupt();

        segmentCurrStateStorage.interrupt();
    }

    /**
     * Interrupt waiting on related objects.
     */
    public void forceInterrupt() {
        segmentArchivedStorage.interrupt();

        segmentCompressStorage.interrupt();

        segmentCurrStateStorage.forceInterrupt();
    }
}
