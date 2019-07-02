/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query memory tracker.
 *
 * Track query memory usage and throws an exception if query tries to allocate memory over limit.
 */
public class QueryMemoryTracker extends H2MemoryTracker implements AutoCloseable {
    /** Resered field updater. */
    private static final AtomicLongFieldUpdater<QueryMemoryTracker> RESERVED_UPD =
        AtomicLongFieldUpdater.newUpdater(QueryMemoryTracker.class, "reserved");

    /** Closed flag updater. */
    private static final AtomicReferenceFieldUpdater<QueryMemoryTracker, Boolean> CLOSED_UPD =
        AtomicReferenceFieldUpdater.newUpdater(QueryMemoryTracker.class, Boolean.class, "closed");

    /** Parent tracker. */
    private final H2MemoryTracker parent;

    /** Query memory limit. */
    private final long maxMem;

    /** Reservation block size. */
    private final long blockSize;

    /** Memory reserved on parent. */
    private volatile long reservedFromParent;

    /** Memory reserved by query. */
    private volatile long reserved;

    /** Close flag to prevent tracker reuse. */
    private volatile Boolean closed = Boolean.FALSE;

    /**
     * Constructor.
     *
     * @param parent Parent memory tracker.
     * @param maxMem Query memory limit in bytes.
     * @param blockSize Reservation block size.
     */
    QueryMemoryTracker(H2MemoryTracker parent, long maxMem, long blockSize) {
        assert maxMem > 0;

        this.parent = parent;
        this.maxMem = maxMem;
        this.blockSize = blockSize;
    }

    /** {@inheritDoc} */
    @Override public void reserve(long size) {
        assert !closed && size >= 0;

        if (size == 0)
            return;

        long reserved0 = RESERVED_UPD.accumulateAndGet(this, size, (prev, x) -> {
            if (prev + x > maxMem) {
                throw new IgniteSQLException("SQL query run out of memory: Query quota exceeded.",
                    IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY);
            }

            return prev + x;
        });

        if (parent != null && reserved0 > reservedFromParent) {
            synchronized (this) {
                assert !closed;

                if (reserved0 <= reservedFromParent)
                    return;

                // If single block size is too small.
                long blockSize = Math.max(reserved0 - reservedFromParent, this.blockSize);
                // If we are too close to limit.
                blockSize = Math.min(blockSize, maxMem - reservedFromParent);

                try {
                    parent.reserve(blockSize);

                    reservedFromParent += blockSize;
                }
                catch (Throwable e) {
                    // Fallback if failed to reserve.
                    RESERVED_UPD.addAndGet(this, -size);

                    throw e;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void release(long size) {
        assert size >= 0;

        if (size == 0)
            return;

        long reserved = RESERVED_UPD.accumulateAndGet(this, -size, (prev, x) -> {
            if (prev + x < 0)
                throw new IllegalStateException("Try to release more memory that were reserved: [" +
                    "reserved=" + prev + ", toRelease=" + x + ']');

            return prev + x;
        });

        assert !closed && reserved >= 0 || reserved == 0 : "Invalid reserved memory size:" + reserved;

        // For now, won'tQ release memory to parent until tracker closed.
       /* if (parent != null && preAllocated - reserved >= 2 * blockSize) {
            synchronized (this) {
                if (preAllocated - reserved >= 2 * blockSize) {
                    parent.release(blockSize);

                    preAllocated -= blockSize;
                }
            }
        }*/
    }

    /**
     * @return {@code True} if closed, {@code False} otherwise.
     */
    public boolean closed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // It is not expected to be called concurrently with reserve\release.
        // But query can be cancelled concurrently on query finish.
        if (CLOSED_UPD.compareAndSet(this, Boolean.FALSE, Boolean.TRUE)) {
            release(RESERVED_UPD.get(this));

            if (parent != null)
                parent.release(reservedFromParent);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryMemoryTracker.class, this);
    }
}