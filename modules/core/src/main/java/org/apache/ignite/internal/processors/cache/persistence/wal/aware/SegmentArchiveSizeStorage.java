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
 * Storage WAL archive size.
 * Allows to track the exceeding of the maximum archive size.
 */
class SegmentArchiveSizeStorage {
    /** Current WAL archive size in bytes. Guarded by {@code this}. */
    private long curr;

    /** Reserved WAL archive size in bytes. Guarded by {@code this}. */
    private long reserved;

    /** Flag of interrupt waiting on this object. Guarded by {@code this}. */
    private boolean interrupted;

    /**
     * Adding WAL archive sizes.
     * Reservation defines a hint to determine if the maximum size is exceeded
     * before the completion of the operation on the segment.
     *
     * @param curr Current WAL archive size in bytes.
     * @param reserved Reserved WAL archive size in bytes.
     */
    synchronized void addSizes(long curr, long reserved) {
        this.curr += curr;
        this.reserved += reserved;

        if (curr > 0 || reserved > 0)
            notifyAll();
    }

    /**
     * Reset the current and reserved WAL archive sizes.
     */
    synchronized void resetSizes() {
        curr = 0;
        reserved = 0;
    }

    /**
     * Waiting for exceeding the maximum WAL archive size.
     * To track size of WAL archive, need to use {@link #addSizes}.
     *
     * @param max Maximum WAL archive size in bytes.
     * @throws IgniteInterruptedCheckedException If it was interrupted.
     */
    synchronized void awaitExceedMaxSize(long max) throws IgniteInterruptedCheckedException {
        try {
            while (max - (curr + reserved) > 0 && !interrupted)
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
}
