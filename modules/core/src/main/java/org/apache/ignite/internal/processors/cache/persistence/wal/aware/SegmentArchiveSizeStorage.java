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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Storage WAL archive size.
 * Allows to track the exceeding of the maximum archive size.
 */
class SegmentArchiveSizeStorage {
    /** Logger. */
    @Nullable private final IgniteLogger log;

    /** Current WAL archive size in bytes. */
    private long curr;

    /** Reserved WAL archive size in bytes. */
    private long reserved;

    /** Flag of interrupt waiting on this object. */
    private volatile boolean interrupted;

    /**
     * Constructor.
     *
     * @param log Logger.
     */
    SegmentArchiveSizeStorage(@Nullable IgniteLogger log) {
        this.log = log;
    }

    /**
     * Constructor.
     */
    public SegmentArchiveSizeStorage() {
        this(null);
    }

    /**
     * Adding current WAL archive size in bytes.
     *
     * @param size Size in bytes.
     */
    synchronized void addCurrentSize(long size) {
        curr += size;

        if (log != null && log.isDebugEnabled())
            log.debug("Add current WAL archive size: " + U.humanReadableByteCount(size));

        if (size > 0)
            notifyAll();
    }

    /**
     * Adding reserved WAL archive size in bytes.
     * Defines a hint to determine if the maximum size is exceeded before a new segment is archived.
     *
     * @param size Size in bytes.
     */
    synchronized void addReservedSize(long size) {
        reserved += size;

        if (log != null && log.isDebugEnabled())
            log.debug("Add reserved WAL archive size: " + U.humanReadableByteCount(size));

        if (size > 0)
            notifyAll();
    }

    /**
     * Reset the current and reserved WAL archive sizes.
     */
    synchronized void resetSizes() {
        curr = 0;
        reserved = 0;

        if (log != null && log.isDebugEnabled())
            log.debug("Reset WAL sizes");
    }

    /**
     * Waiting for exceeding the maximum WAL archive size.
     * To track size of WAL archive, need to use {@link #addCurrentSize} and {@link #addReservedSize}.
     *
     * @param max Maximum WAL archive size in bytes.
     * @throws IgniteInterruptedCheckedException If it was interrupted.
     */
    synchronized void awaitExceedMaxSize(long max) throws IgniteInterruptedCheckedException {
        try {
            while (max - (curr + reserved) > 0 && !interrupted) {
                if (log != null && log.isDebugEnabled()) {
                    log.debug("Not exceed WAL archive size [max=" + U.humanReadableByteCount(max) +
                        ", total=" + U.humanReadableByteCount(totalSize()) + ']');
                }

                wait();
            }
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

        if (log != null && log.isDebugEnabled())
            log.debug("Interrupt WAL sizes");

        notifyAll();
    }

    /**
     * Reset interrupted flag.
     */
    void reset() {
        interrupted = false;

        if (log != null && log.isDebugEnabled())
            log.debug("Continue WAL sizes");
    }

    /**
     * Getting the total WAL archive size.
     *
     * @return Size in bytes.
     */
    long totalSize() {
        return curr + reserved;
    }
}
