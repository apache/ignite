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
 * Holder of actual information of latest manipulation on WAL segments.
 */
public class SegmentAware {
    /** Segment reservations storage: Protects WAL segments from deletion during WAL log cleanup. */
    private final SegmentReservationStorage reservationStorage = new SegmentReservationStorage();

    /** Lock on segment protects from archiving segment. */
    private final SegmentLockStorage segmentLockStorage = new SegmentLockStorage();

    /** Manages last archived index, emulates archivation in no-archiver mode. */
    private final SegmentArchivedStorage segmentArchivedStorage;

    /** Storage of actual information about current index of compressed segments. */
    private final SegmentCompressStorage segmentCompressStorage;

    /** Storage of absolute current segment index. */
    private final SegmentCurrentStateStorage segmentCurrStateStorage;

    /**
     * Constructor.
     *
     * @param walSegmentsCnt Total WAL segments count.
     * @param compactionEnabled Is wal compaction enabled.
     */
    public SegmentAware(int walSegmentsCnt, boolean compactionEnabled) {
        segmentArchivedStorage = new SegmentArchivedStorage(segmentLockStorage);

        segmentCurrStateStorage = new SegmentCurrentStateStorage(walSegmentsCnt);
        segmentCompressStorage = new SegmentCompressStorage(compactionEnabled);

        segmentArchivedStorage.addObserver(segmentCurrStateStorage::onSegmentArchived);
        segmentArchivedStorage.addObserver(segmentCompressStorage::onSegmentArchived);

        segmentLockStorage.addObserver(segmentArchivedStorage::onSegmentUnlocked);
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
        long idx;

        while ((idx = segmentCompressStorage.nextSegmentToCompressOrWait()) <= lastTruncatedArchiveIdx())
            onSegmentCompressed(idx);

        return idx;
    }

    /**
     * Callback after segment compression finish.
     *
     * @param compressedIdx Index of compressed segment.
     */
    public void onSegmentCompressed(long compressedIdx) {
        segmentCompressStorage.onSegmentCompressed(compressedIdx);
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
        segmentArchivedStorage.lastTruncatedArchiveIdx(lastTruncatedArchiveIdx);
    }

    /**
     * @return Last truncated segment.
     */
    public long lastTruncatedArchiveIdx() {
        return segmentArchivedStorage.lastTruncatedArchiveIdx();
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
     * Segment reservation. It will be successful if segment is {@code >} than
     * the {@link #minReserveIndex minimum}.
     * 
     * @param absIdx Index for reservation.
     * @return {@code True} if the reservation was successful.
     */
    public boolean reserve(long absIdx) {
        return reservationStorage.reserve(absIdx);
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
     * Check if WAL segment locked (protected from move to archive).
     *
     * @param absIdx Index for check locking.
     * @return {@code True} if index is locked.
     */
    public boolean locked(long absIdx) {
        return segmentLockStorage.locked(absIdx);
    }

    /**
     * Segment lock. It will be successful if segment is {@code >} than
     * the {@link #lastArchivedAbsoluteIndex last archived}.
     *
     * @param absIdx Index to lock.
     * @return {@code True} if the lock was successful.
     */
    public boolean lock(long absIdx) {
        return segmentLockStorage.lockWorkSegment(absIdx);
    }

    /**
     * @param absIdx Index to unlock.
     */
    public void unlock(long absIdx) {
        segmentLockStorage.releaseWorkSegment(absIdx);
    }

    /**
     * Reset interrupted flag.
     */
    public void reset() {
        segmentArchivedStorage.reset();

        segmentCompressStorage.reset();

        segmentCurrStateStorage.reset();
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

    /**
     * Increasing minimum segment index after that can be reserved.
     * Value will be updated if it is greater than the current one.
     * If segment is already reserved, the update will fail.
     *
     * @param absIdx Absolut segment index.
     * @return {@code True} if update is successful.
     */
    public boolean minReserveIndex(long absIdx) {
        return reservationStorage.minReserveIndex(absIdx);
    }

    /**
     * Increasing minimum segment index after that can be locked.
     * Value will be updated if it is greater than the current one.
     * If segment is already reserved, the update will fail.
     *
     * @param absIdx Absolut segment index.
     * @return {@code True} if update is successful.
     */
    public boolean minLockIndex(long absIdx) {
        return segmentLockStorage.minLockIndex(absIdx);
    }
}
