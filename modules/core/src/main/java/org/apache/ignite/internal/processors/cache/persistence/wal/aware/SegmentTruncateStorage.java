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
 * Store the last truncated segment and allows to get the number of segments available for truncation.
 * We cannot truncate the segments required for {@link #lastCpIdx binary recovery}, {@link #minReservedIdx reserved}
 * and {@link #lastArchivedIdx last archived} (to restart the node correctly). Thus, we need to take account of these
 * conditions in the calculation of the number of segments available for truncation.
 */
class SegmentTruncateStorage {
    /** Flag of interrupt waiting on this object. */
    private volatile boolean interrupted;

    /** Latest truncated segment. */
    private long lastTruncatedIdx = -1;

    /** Minimum reserved segment. */
    private long minReservedIdx = -1;

    /** Segment of last completed checkpoint. */
    private long lastCpIdx = -1;

    /** Last archived segment. */
    private long lastArchivedIdx = -1;

    /**
     * Update last truncated segment.
     *
     * @param absIdx Absolut segment index.
     */
    synchronized void lastTruncatedIdx(long absIdx) {
        lastTruncatedIdx = absIdx;

        notifyAll();
    }

    /**
     * Update minimum reserved segment.
     * Protected from deletion.
     *
     * @param absIdx Absolut segment index.
     */
    synchronized void minReservedIdx(long absIdx) {
        minReservedIdx = absIdx;

        notifyAll();
    }

    /**
     * Update segment of last completed checkpoint.
     * Required for binary recovery.
     *
     * @param absIdx Absolut segment index.
     */
    synchronized void lastCheckpointIdx(long absIdx) {
        lastCpIdx = absIdx;

        notifyAll();
    }

    /**
     * Update last archived segment.
     * Needed to restart the node correctly.
     *
     * @param absIdx Absolut segment index.
     */
    synchronized void lastArchivedIdx(long absIdx) {
        lastArchivedIdx = absIdx;

        notifyAll();
    }

    /**
     * Getting last truncated segment.
     *
     * @return Absolut segment index.
     */
    synchronized long lastTruncatedIdx() {
        return lastTruncatedIdx;
    }

    /**
     * Waiting for segment truncation to be available. Use {@link #lastTruncatedIdx}, {@link #lastCpIdx},
     * {@link #minReservedIdx} and {@link #lastArchivedIdx} to determine the number of segments to truncate.
     *
     * @return Number of segments available to truncate.
     * @throws IgniteInterruptedCheckedException If it was interrupted.
     */
    synchronized long awaitAvailableTruncate() throws IgniteInterruptedCheckedException {
        try {
            while (availableTruncateCnt() == 0 && !interrupted)
                wait();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }

        if (interrupted)
            throw new IgniteInterruptedCheckedException("Interrupt waiting for truncation availability");

        return availableTruncateCnt();
    }

    /**
     * Interrupt waiting on this object.
     */
    synchronized void interrupt() {
        interrupted = true;

        notifyAll();
    }

    /**
     * Resets interrupted flag.
     */
    void reset() {
        interrupted = false;
    }

    /**
     * Calculation the number of segments that can be truncated.
     *
     * @return Number of segments.
     */
    private synchronized long availableTruncateCnt() {
        long highIdx = minReservedIdx == -1 ? lastCpIdx : Math.min(minReservedIdx, lastCpIdx);

        // Protection against deleting the last segment from WAL archive for correct restart the node.
        highIdx = lastArchivedIdx == -1 ? highIdx : Math.min(lastArchivedIdx, highIdx);

        return Math.max(0, highIdx == -1 ? 0 : highIdx - (lastTruncatedIdx + 1));
    }
}
