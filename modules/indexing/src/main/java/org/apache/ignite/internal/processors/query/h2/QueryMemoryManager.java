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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.metric.SqlStatisticsHolderMemoryQuotas;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.util.IgniteUtils.KB;

/**
 * Query memory manager.
 */
public class QueryMemoryManager extends H2MemoryTracker {
    //TODO: GG-18629: Move defaults to memory quotas configuration.
    /**
     * Default memory reservation block size.
     */
    private static final long DFLT_MEMORY_RESERVATION_BLOCK_SIZE = 512 * KB;

    /** Set of metrics that collect info about memory this memory manager tracks. */
    private final SqlStatisticsHolderMemoryQuotas metrics;

    /** */
    static final LongBinaryOperator RELEASE_OP = new LongBinaryOperator() {
        @Override public long applyAsLong(long prev, long x) {
            long res = prev - x;

            if (res < 0)
                throw new IllegalStateException("Try to free more memory that ever be reserved: [" +
                    "reserved=" + prev + ", toFree=" + x + ']');

            return res;
        }
    };

    /**
     * Default query memory limit.
     *
     * Note: Actually, it is  per query (Map\Reduce) stage limit. With QueryParallelism every query-thread will be
     * treated as separate Map query.
     */
    private final long dfltSqlQryMemoryLimit;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private final LongBinaryOperator reserveOp;

    /** Global query memory quota. */
    //TODO GG-18629: it looks safe to make this configurable at runtime.
    private final long globalQuota;

    /** Reservation block size. */
    //TODO GG-18629: it looks safe to make this configurable at runtime.
    private final long blockSize;

    /** Memory reserved by running queries. */
    private final AtomicLong reserved = new AtomicLong();

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param globalQuota Node memory available for sql queries.
     */
    public QueryMemoryManager(GridKernalContext ctx, long globalQuota) {
        if (Runtime.getRuntime().maxMemory() <= globalQuota)
            throw new IllegalStateException("Sql memory pool size can't be more than heap memory max size.");

        if (globalQuota == 0) {
            globalQuota = Long.getLong(IgniteSystemProperties.IGNITE_DEFAULT_SQL_MEMORY_POOL_SIZE,
                (long)(Runtime.getRuntime().maxMemory() * 0.6d));
        }

        long dfltMemLimit = Long.getLong(IgniteSystemProperties.IGNITE_DEFAULT_SQL_QUERY_MEMORY_LIMIT, 0);

        if (dfltMemLimit == 0)
            dfltMemLimit = globalQuota > 0 ? globalQuota / IgniteConfiguration.DFLT_QUERY_THREAD_POOL_SIZE : -1;

        this.blockSize = Long.getLong(IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE, DFLT_MEMORY_RESERVATION_BLOCK_SIZE);
        this.globalQuota = globalQuota;
        this.dfltSqlQryMemoryLimit = dfltMemLimit;

        this.reserveOp = new ReservationOp(globalQuota);

        this.log = ctx.log(QueryMemoryManager.class);

        metrics = new SqlStatisticsHolderMemoryQuotas(this, ctx.metric());
    }

    /** {@inheritDoc} */
    @Override public void reserve(long size) {
        if (size == 0)
            return; // Nothing to do.

        assert size > 0;

        reserved.accumulateAndGet(size, reserveOp);

        metrics.trackReserve(size);
    }

    /** {@inheritDoc} */
    @Override public void release(long size) {
        if (size == 0)
            return; // Nothing to do.

        assert size > 0;

        reserved.accumulateAndGet(size, RELEASE_OP);
    }

    /**
     * Query memory tracker factory method.
     *
     * Note: If 'maxQueryMemory' is zero, then {@link QueryMemoryManager#dfltSqlQryMemoryLimit}  will be used.
     * Note: Negative values are reserved for disable memory tracking.
     *
     * @param maxQueryMemory Query memory limit in bytes.
     * @return Query memory tracker.
     */
    public QueryMemoryTracker createQueryMemoryTracker(long maxQueryMemory) {
        assert maxQueryMemory >= 0;

        if (maxQueryMemory == 0)
            maxQueryMemory = dfltSqlQryMemoryLimit;

        if (dfltSqlQryMemoryLimit < 0)
            return null;

        if (globalQuota > 0 && globalQuota < maxQueryMemory) {
            U.warn(log, "Max query memory can't exceeds SQL memory pool size. Will be reduced down to: " + globalQuota);

            maxQueryMemory = globalQuota;
        }

        return new QueryMemoryTracker(globalQuota < 0 ? null : this, maxQueryMemory, Math.min(maxQueryMemory, blockSize));
    }

    /**
     * Gets memory reserved by running queries.
     *
     * @return Reserved memory in bytes.
     */
    public long memoryReserved() {
        return reserved.get();
    }

    /**
     * Gets global memory limit for queries.
     *
     * @return Max memory in bytes.
     */
    public long maxMemory() {
        return globalQuota;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Cursors are not tracked and can't be forcibly closed to release resources.
        // For now, it is ok as neither extra memory is actually hold with MemoryManager nor file descriptors are used.
        if (log.isDebugEnabled() && reserved.get() != 0)
            log.debug("Potential memory leak in SQL processor. Some query cursors were not closed or forget to free memory.");
    }

    /** */
    private static class ReservationOp implements LongBinaryOperator {
        /** Operation result high bound.*/
        private final long limit;

        /**
         * Constructor.
         * @param limit Operation result high bound.
         */
        ReservationOp(long limit) {
            this.limit = limit;
        }

        /** {@inheritDoc} */
        @Override public long applyAsLong(long prev, long x) {
            long res = prev + x;

            if (res > limit)
                throw new IgniteSQLException("SQL query run out of memory: Global quota exceeded.",
                    IgniteQueryErrorCode.QUERY_OUT_OF_MEMORY);

            return res;
        }
    }
}
