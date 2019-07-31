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

import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query memory tracker.
 *
 * Track query memory usage and throws an exception if query tries to allocate memory over limit.
 */
public class QueryMemoryTracker extends H2MemoryTracker implements AutoCloseable {
    /** Parent tracker. */
    private final H2MemoryTracker parent;

    /** Query memory limit. */
    private final long maxMem;

    /** Reservation block size. */
    private final long blockSize;

    /** Memory reserved on parent. */
    private long reservedFromParent;

    /** Memory reserved by query. */
    private long reserved;

    /** Close flag to prevent tracker reuse. */
    private Boolean closed = Boolean.FALSE;

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
        if (size == 0)
            return;

        assert size > 0;

        synchronized (this) {
            if (closed)
                throw new IllegalStateException("Memory tracker has been closed concurrently.");

            long reserved0 = reserve0(size);

            if (parent != null && reserved0 > reservedFromParent) {
                try {
                    // If single block size is too small.
                    long blockSize = Math.max(reserved0 - reservedFromParent, this.blockSize);
                    // If we are too close to limit.
                    blockSize = Math.min(blockSize, maxMem - reservedFromParent);

                    parent.reserve(blockSize);

                    reservedFromParent += blockSize;
                }
                catch (Throwable e) {
                    // Fallback if failed to reserve.
                    release0(size);

                    throw e;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void release(long size) {
        if (size == 0)
            return;

        assert size > 0;

        synchronized (this) {
            if (closed)
                throw new IllegalStateException("Memory tracker has been closed concurrently.");

            long reserved = release0(size);

            assert reserved >= 0;
        }
    }

    /**
     * @param size Memory to reserve in bytes.
     * @return Reserved memory after release.
     */
    private long reserve0(long size) {
        long res = reserved + size;

        if (res > maxMem)
            throw new IgniteSQLException("SQL query run out of memory: Query quota exceeded.",
                IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY);

        return reserved = res;
    }

    /**
     * Release reserved memory.
     *
     * @param size Memory to release in bytes.
     * @return Reserved memory after release.
     */
    private long release0(long size) {

        long res = reserved - size;

        if (res < 0)
            throw new IllegalStateException("Try to free more memory that ever be reserved: [" +
                "reserved=" + reserved + ", toFree=" + size + ']');

        return reserved = res;
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
        synchronized (this) {
            if (closed)
                return;

            closed = true;

            release0(reserved);

            if (parent != null)
                parent.release(reservedFromParent);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryMemoryTracker.class, this);
    }
}