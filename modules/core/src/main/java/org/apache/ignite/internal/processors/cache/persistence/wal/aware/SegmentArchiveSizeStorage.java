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

import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;

/**
 * Storage WAL archive size.
 * Allows to track the exceeding of the maximum archive size.
 */
class SegmentArchiveSizeStorage {
    /** Current WAL archive size in bytes. Guarded by {@code this}. */
    private long curr;

    /** Flag of interrupt waiting on this object. Guarded by {@code this}. */
    private boolean interrupted;

    /** Minimum size of the WAL archive in bytes. */
    private final long minWalArchiveSize;

    /** Maximum size of the WAL archive in bytes. */
    private final long maxWalArchiveSize;

    /**
     * Segment sizes. Mapping: segment idx -> size in bytes. Guarded by {@code this}.
     * {@code null} if {@link #maxWalArchiveSize} == {@link DataStorageConfiguration#UNLIMITED_WAL_ARCHIVE}.
     */
    @Nullable private final Map<Long, Long> segmentSize;

    /**
     * Segment reservations storage.
     * {@code null} if {@link #maxWalArchiveSize} == {@link DataStorageConfiguration#UNLIMITED_WAL_ARCHIVE}.
     */
    @Nullable private final SegmentReservationStorage reservationStorage;

    /**
     * Constructor.
     *
     * @param minWalArchiveSize Minimum size of the WAL archive in bytes.
     * @param maxWalArchiveSize Maximum size of the WAL archive in bytes
     *      or {@link DataStorageConfiguration#UNLIMITED_WAL_ARCHIVE}.
     * @param reservationStorage Segment reservations storage.
     */
    public SegmentArchiveSizeStorage(
        long minWalArchiveSize,
        long maxWalArchiveSize,
        SegmentReservationStorage reservationStorage
    ) {
        this.minWalArchiveSize = minWalArchiveSize;
        this.maxWalArchiveSize = maxWalArchiveSize;

        if (maxWalArchiveSize != UNLIMITED_WAL_ARCHIVE) {
            segmentSize = new TreeMap<>();
            this.reservationStorage = reservationStorage;
        }
        else {
            segmentSize = null;
            this.reservationStorage = null;
        }
    }

    /**
     * Adding the WAL segment size in the archive.
     *
     * @param idx Absolut segment index.
     * @param curr Current WAL archive size in bytes.
     */
    void addSize(long idx, long curr) {
        long releaseIdx = -1;

        synchronized (this) {
            this.curr += curr;

            if (segmentSize != null) {
                segmentSize.compute(idx, (i, size) -> {
                    long res = (size == null ? 0 : size) + curr;

                    return res == 0 ? null : res;
                });
            }

            if (curr > 0) {
                if (segmentSize != null && this.curr >= maxWalArchiveSize) {
                    long size = 0;

                    for (Map.Entry<Long, Long> e : segmentSize.entrySet()) {
                        releaseIdx = e.getKey();

                        if (this.curr - (size += e.getValue()) < minWalArchiveSize)
                            break;
                    }
                }

                notifyAll();
            }
        }

        if (releaseIdx != -1) {
            assert reservationStorage != null;

            reservationStorage.forceRelease(releaseIdx);
        }
    }

    /**
     * Reset the current and reserved WAL archive sizes.
     */
    synchronized void resetSizes() {
        curr = 0;

        if (segmentSize != null)
            segmentSize.clear();
    }

    /**
     * Waiting for exceeding the maximum WAL archive size.
     * To track size of WAL archive, need to use {@link #addSize}.
     *
     * @param max Maximum WAL archive size in bytes.
     * @throws IgniteInterruptedCheckedException If it was interrupted.
     */
    synchronized void awaitExceedMaxSize(long max) throws IgniteInterruptedCheckedException {
        try {
            while (max - curr > 0 && !interrupted)
                wait();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }

        if (interrupted)
            throw new IgniteInterruptedCheckedException("Interrupt waiting of exceed max archive size");
    }

    /**
     * Interrupt waiting on this object.
     */
    synchronized void interrupt() {
        interrupted = true;

        notifyAll();
    }

    /**
     * Reset interrupted flag.
     */
    synchronized void reset() {
        interrupted = false;
    }

    /**
     * Getting current WAL archive size in bytes.
     *
     * @return Size in bytes.
     */
    synchronized long currentSize() {
        return curr;
    }

    /**
     * Getting the size of the WAL segment of the archive in bytes.
     *
     * @return Size in bytes or {@code null} if the segment is absent or the archive is unlimited.
     */
    @Nullable Long segmentSize(long idx) {
        if (segmentSize == null)
            return null;
        else {
            synchronized (this) {
                return segmentSize.get(idx);
            }
        }
    }
}
