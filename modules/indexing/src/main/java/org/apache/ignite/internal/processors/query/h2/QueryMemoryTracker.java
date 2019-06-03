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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query memory tracker.
 *
 * Track query memory usage and throws an exception if query tries to allocate memory over limit.
 */
public class QueryMemoryTracker extends H2MemoryTracker implements AutoCloseable {
    //TODO: GG-18629: Move defaults to memory quotas configuration.
    /**
     * Default query memory limit.
     *
     * Note: Actually, it is  per query (Map\Reduce) stage limit. With QueryParallelism every query-thread will be
     * treated as separate Map query.
     */
    public static final long DFLT_QRY_MEMORY_LIMIT = Long.getLong(IgniteSystemProperties.IGNITE_SQL_QUERY_MEMORY_LIMIT,
        (long)(Runtime.getRuntime().maxMemory() * 0.6d / IgniteConfiguration.DFLT_QUERY_THREAD_POOL_SIZE));

    /** Allocated field updater. */
    private static final AtomicLongFieldUpdater<QueryMemoryTracker> ALLOC_UPD =
        AtomicLongFieldUpdater.newUpdater(QueryMemoryTracker.class, "allocated");

    /** Closed flag updater. */
    private static final AtomicReferenceFieldUpdater<QueryMemoryTracker, Boolean> CLOSED_UPD =
        AtomicReferenceFieldUpdater.newUpdater(QueryMemoryTracker.class, Boolean.class, "closed");

    /** Memory limit. */
    private final long maxMem;

    /** Memory allocated. */
    private volatile long allocated;

    /** Close flag to prevent tracker reuse. */
    private volatile Boolean closed = Boolean.FALSE;

    /**
     * Constructor.
     *
     * @param maxMem Query memory limit in bytes. Note: If zero value, then {@link QueryMemoryTracker#DFLT_QRY_MEMORY_LIMIT}
     * will be used. Note: Negative values are reserved for disable memory tracking.
     */
    public QueryMemoryTracker(long maxMem) {
        assert maxMem >= 0;

        this.maxMem = maxMem > 0 ? maxMem : DFLT_QRY_MEMORY_LIMIT;
    }

    /**
     * Check allocated size is less than query memory pool threshold.
     *
     * @param size Allocated size in bytes.
     * @throws IgniteOutOfMemoryException if memory limit has been exceeded.
     */
    @Override public void allocate(long size) {
        assert !closed && size >= 0;

        if (size == 0)
            return;

        if (ALLOC_UPD.addAndGet(this, size) >= maxMem)
            throw new IgniteOutOfMemoryException("SQL query out of memory");
    }

    /** {@inheritDoc} */
    @Override public void free(long size) {
        assert size >= 0;

        if (size == 0)
            return;

        long allocated = ALLOC_UPD.addAndGet(this, -size);

        assert !closed && allocated >= 0 || allocated == 0 : "Invalid allocated memory size:" + allocated;
    }

    /**
     * @return Memory allocated by tracker.
     */
    public long getAllocated() {
        return allocated;
    }

    /**
     * @return {@code True} if closed, {@code False} otherwise.
     */
    public boolean closed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // It is not expected to be called concurrently with allocate\free.
        if (CLOSED_UPD.compareAndSet(this, Boolean.FALSE, Boolean.TRUE))
            free(allocated);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryMemoryTracker.class, this);
    }
}