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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.indexing.IndexesRebuildTask;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexOperationCancellationToken;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.jetbrains.annotations.Nullable;

/**
 * Extension {@link IndexesRebuildTask} for the tests.
 */
class IndexesRebuildTaskEx extends IndexesRebuildTask {
    /** Consumer for cache rows when rebuilding indexes. */
    static final Map<String, IgniteThrowableConsumer<CacheDataRow>> cacheRowConsumer = new ConcurrentHashMap<>();

    /** A function that should run before preparing to rebuild the cache indexes. */
    static final Map<String, Runnable> cacheRebuildRunner = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected void startRebuild(
        GridCacheContext cctx,
        GridFutureAdapter<Void> rebuildIdxFut,
        SchemaIndexCacheVisitorClosure clo,
        SchemaIndexOperationCancellationToken cancel
    ) {
        super.startRebuild(cctx, rebuildIdxFut, new SchemaIndexCacheVisitorClosure() {
            /** {@inheritDoc} */
            @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
                cacheRowConsumer.getOrDefault(cctx.name(), r -> { }).accept(row);

                clo.apply(row);
            }
        }, cancel);
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteInternalFuture<?> rebuild(GridCacheContext cctx, boolean force) {
        cacheRebuildRunner.getOrDefault(cctx.name(), () -> { }).run();

        return super.rebuild(cctx, force);
    }

    /**
     * Cleaning of internal structures.
     */
    static void clean() {
        cacheRowConsumer.clear();
        cacheRebuildRunner.clear();
    }

    /**
     * Consumer for stopping rebuilding indexes of cache.
     */
    static class StopRebuildIndexConsumer implements IgniteThrowableConsumer<CacheDataRow> {
        /** Future to indicate that the rebuilding indexes has begun. */
        final GridFutureAdapter<Void> startRebuildIdxFut = new GridFutureAdapter<>();

        /** Future to wait to continue rebuilding indexes. */
        final GridFutureAdapter<Void> finishRebuildIdxFut = new GridFutureAdapter<>();

        /** Counter of visits. */
        final AtomicLong visitCnt = new AtomicLong();

        /** The maximum time to wait finish future in milliseconds. */
        final long timeout;

        /**
         * Constructor.
         *
         * @param timeout The maximum time to wait finish future in milliseconds.
         */
        StopRebuildIndexConsumer(long timeout) {
            this.timeout = timeout;
        }

        /** {@inheritDoc} */
        @Override public void accept(CacheDataRow row) throws IgniteCheckedException {
            startRebuildIdxFut.onDone();

            visitCnt.incrementAndGet();

            finishRebuildIdxFut.get(timeout);
        }

        /**
         * Resetting internal futures.
         */
        void resetFutures() {
            startRebuildIdxFut.reset();
            finishRebuildIdxFut.reset();
        }
    }
}
