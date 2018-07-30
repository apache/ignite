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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;

/**
 *
 */
public class SegmentAware {
    /** Latest segment cleared by {@link #truncate(WALPointer, WALPointer)}. */
    private volatile long lastTruncatedArchiveIdx = -1L;

    /** Segment reservations storage: Protects WAL segments from deletion during WAL log cleanup. */
    private final SegmentReservationStorage reservationStorage = new SegmentReservationStorage();

    /** Manages last archived index, emulates archivation in no-archiver mode. */
    private final SegmentLockStorage segmentLockStorage = new SegmentLockStorage();

    /** Manages last archived index, emulates archivation in no-archiver mode. */
    private final SegmentArchivedStorage archivedMonitor = new SegmentArchivedStorage(segmentLockStorage);

    private final SegmentCompressStorage segmentCompressStorage = new SegmentCompressStorage(archivedMonitor);

    private final SegmentCurrentStorage segmentCurrentStorage;

    public SegmentAware(int count) {
        segmentCurrentStorage = new SegmentCurrentStorage(count, archivedMonitor);
    }

    public void reserve(long absIdx) {
        reservationStorage.reserve(absIdx);
    }

    public boolean reserved(long absIdx) {
        return reservationStorage.reserved(absIdx);
    }

    public void release(long absIdx) {
        reservationStorage.release(absIdx);
    }

    public void awaitSegment(long absSegIdx) throws InterruptedException, IgniteInterruptedCheckedException {
        segmentCurrentStorage.awaitSegment(absSegIdx);
    }

    public void curAbsWalIdx(long curAbsWalIdx) {
        segmentCurrentStorage.curAbsWalIdx(curAbsWalIdx);
    }

    /**
     * @param lastAbsArchivedIdx new value of last archived segment index
     */
    public void markAsMovedToArchive(long toArchive) throws InterruptedException, IgniteInterruptedCheckedException {
        archivedMonitor.markAsMovedToArchive(toArchive);
    }

    /**
     * @param lastAbsArchivedIdx new value of last archived segment index
     */
    public long nextAbsoluteSegmentIndex() throws InterruptedException, IgniteInterruptedCheckedException {
        return segmentCurrentStorage.nextAbsoluteSegmentIndex();
    }

    public long waitNextArchiveSegment() throws InterruptedException, IgniteInterruptedCheckedException {
//            assert lastAbsArchivedIdx() <= curAbsWalIdx : "lastArchived=" + lastAbsArchivedIdx() +
//                ", current=" + curAbsWalIdx;

            long nextArchivedIdx = lastArchivedAbsoluteIndex();

            awaitSegment(nextArchivedIdx + 2);

            return nextArchivedIdx + 1;
    }

    public void allowCompressionUntil(long lastCpStartIdx) {
         segmentCompressStorage.allowCompressionUntil(lastCpStartIdx);
    }

    public void lastCompressedIdx(long lastCompressedIdx) {
        segmentCompressStorage.lastCompressedIdx(lastCompressedIdx);
    }

    public long lastCompressedIdx() {
        return segmentCompressStorage.lastCompressedIdx();
    }

    public void lastTruncatedArchiveIdx(long lastTruncatedArchiveIdx) {
        this.lastTruncatedArchiveIdx = lastTruncatedArchiveIdx;
    }

    public long lastTruncatedArchiveIdx() {
        return lastTruncatedArchiveIdx;
    }



    public long curAbsWalIdx() {
        return segmentCurrentStorage.curAbsWalIdx();
    }

    /**
     * Pessimistically tries to reserve segment for compression in order to avoid concurrent truncation.
     * Waits if there's no segment to archive right now.
     */
    public long nextSegmentToCompressOrWait() throws InterruptedException, IgniteCheckedException, IgniteInterruptedCheckedException {
        return Math.max(segmentCompressStorage.nextSegmentToCompressOrWait(), lastTruncatedArchiveIdx + 1);
    }

    /**
     * @return Last archived segment absolute index.
     */
    public long lastArchivedAbsoluteIndex() {
        return archivedMonitor.lastArchivedAbsoluteIndex();
    }

    /**
     * @param lastAbsArchivedIdx new value of last archived segment index
     */
    public void setLastArchivedAbsoluteIndex(long lastAbsArchivedIdx) {
        archivedMonitor.setLastArchivedAbsoluteIndex(lastAbsArchivedIdx);
    }

    public void stop() {
        archivedMonitor.stop();

        segmentCompressStorage.stop();

        segmentCurrentStorage.stop();
    }

    /**
     * Method will wait activation of particular WAL segment index.
     *
     * @param awaitIdx absolute index  {@link #lastArchivedAbsoluteIndex()} to become true.
     * @throws IgniteInterruptedCheckedException if interrupted.
     */
    public void awaitSegmentArchived(long awaitIdx) throws IgniteInterruptedCheckedException {
        archivedMonitor.awaitSegmentArchived(awaitIdx);
    }

    public boolean locked(long absIdx) {
        return segmentLockStorage.locked(absIdx);
    }

    /**
     * @param absIdx Segment absolute index.
     * @return <ul><li>{@code True} if can read, no lock is held, </li><li>{@code false} if work segment, need
     * release segment later, use {@link #releaseWorkSegment} for unlock</li> </ul>
     */
    public boolean checkCanReadArchiveOrReserveWorkSegment(long absIdx) {
        return lastArchivedAbsoluteIndex() >= absIdx || lockWorkSegment(absIdx);
    }

    /**
     * @param absIdx Segment absolute index.
     * @return <ul><li>{@code True} if can read, no lock is held, </li><li>{@code false} if work segment, need release
     * segment later, use {@link #releaseWorkSegment} for unlock</li> </ul>
     */
    public boolean lockWorkSegment(long absIdx) {
        return segmentLockStorage.lockWorkSegment(absIdx);
    }

    /**
     * @param absIdx Segment absolute index.
     */
    public void releaseWorkSegment(long absIdx) {
        segmentLockStorage.releaseWorkSegment(absIdx);
    }

}
