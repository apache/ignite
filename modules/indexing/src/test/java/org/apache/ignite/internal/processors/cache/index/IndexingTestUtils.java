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

package org.apache.ignite.internal.processors.cache.index;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableBiPredicate;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Utility class for indexing.
 */
class IndexingTestUtils {
    /** A do-nothing {@link CacheDataRow} consumer. */
    static final IgniteThrowableConsumer<CacheDataRow> DO_NOTHING_CACHE_DATA_ROW_CONSUMER = row -> {
    };

    /**
     * Private constructor.
     */
    private IndexingTestUtils() {
        // No-op.
    }

    /**
     * Getting local instance name of the node.
     *
     * @param kernalCtx Kernal context.
     * @return Local instance name.
     */
    static String nodeName(GridKernalContext kernalCtx) {
        return kernalCtx.igniteInstanceName();
    }

    /**
     * Getting local instance name of the node.
     *
     * @param n Node.
     * @return Local instance name.
     */
    static String nodeName(IgniteEx n) {
        return nodeName(n.context());
    }

    /**
     * Getting local instance name of the node.
     *
     * @param cacheCtx Cache context.
     * @return Local instance name.
     */
    static String nodeName(GridCacheContext cacheCtx) {
        return nodeName(cacheCtx.kernalContext());
    }

    /**
     * Consumer for stopping building indexes of cache.
     */
    static class StopBuildIndexConsumer implements IgniteThrowableConsumer<CacheDataRow> {
        /** Future to indicate that the building indexes has begun. */
        final GridFutureAdapter<Void> startBuildIdxFut = new GridFutureAdapter<>();

        /** Future to wait to continue building indexes. */
        final GridFutureAdapter<Void> finishBuildIdxFut = new GridFutureAdapter<>();

        /** Counter of visits. */
        final AtomicLong visitCnt = new AtomicLong();

        /** The maximum time to wait finish future in milliseconds. */
        final long timeout;

        /**
         * Constructor.
         *
         * @param timeout The maximum time to wait finish future in milliseconds.
         */
        StopBuildIndexConsumer(long timeout) {
            this.timeout = timeout;
        }

        /** {@inheritDoc} */
        @Override public void accept(CacheDataRow row) throws IgniteCheckedException {
            startBuildIdxFut.onDone();

            visitCnt.incrementAndGet();

            finishBuildIdxFut.get(timeout);
        }

        /**
         * Resetting internal futures.
         */
        void resetFutures() {
            startBuildIdxFut.reset();
            finishBuildIdxFut.reset();
        }
    }

    /**
     * Consumer for slowdown building indexes of cache.
     */
    static class SlowdownBuildIndexConsumer extends StopBuildIndexConsumer {
        /** Sleep time after processing each cache row in milliseconds. */
        final AtomicLong sleepTime;

        /**
         * Constructor.
         *
         * @param timeout The maximum time to wait finish future in milliseconds.
         * @param sleepTime Sleep time after processing each cache row in milliseconds.
         */
        SlowdownBuildIndexConsumer(long timeout, long sleepTime) {
            super(timeout);

            this.sleepTime = new AtomicLong(sleepTime);
        }

        /** {@inheritDoc} */
        @Override public void accept(CacheDataRow row) throws IgniteCheckedException {
            super.accept(row);

            long sleepTime = this.sleepTime.get();

            if (sleepTime > 0)
                U.sleep(sleepTime);
        }
    }

    /**
     * Consumer breaking index building for the cache.
     */
    static class BreakBuildIndexConsumer extends StopBuildIndexConsumer {
        /** Predicate for throwing an {@link IgniteCheckedException}. */
        final IgniteThrowableBiPredicate<BreakBuildIndexConsumer, CacheDataRow> brakePred;

        /**
         * Constructor.
         *
         * @param timeout The maximum time to wait finish future in milliseconds.
         * @param brakePred Predicate for throwing an {@link IgniteCheckedException}.
         */
        BreakBuildIndexConsumer(
            long timeout,
            IgniteThrowableBiPredicate<BreakBuildIndexConsumer, CacheDataRow> brakePred
        ) {
            super(timeout);

            this.brakePred = brakePred;
        }

        /** {@inheritDoc} */
        @Override public void accept(CacheDataRow row) throws IgniteCheckedException {
            super.accept(row);

            if (brakePred.test(this, row))
                throw new IgniteCheckedException("From test.");
        }
    }
}
