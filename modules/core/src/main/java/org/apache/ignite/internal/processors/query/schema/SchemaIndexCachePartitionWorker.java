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

package org.apache.ignite.internal.processors.query.schema;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_INDEX_REBUILD_BATCH_SIZE;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;
import static org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter.RowData.KEY_ONLY;

/**
 * Worker for creating/rebuilding indexes for cache per partition.
 */
public class SchemaIndexCachePartitionWorker extends GridWorker {
    /** Default count of rows, being processed within a single checkpoint lock. */
    public static final int DFLT_IGNITE_INDEX_REBUILD_BATCH_SIZE = 1_000;

    /** Count of rows, being processed within a single checkpoint lock. */
    private final int batchSize = getInteger(IGNITE_INDEX_REBUILD_BATCH_SIZE, DFLT_IGNITE_INDEX_REBUILD_BATCH_SIZE);

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Stop flag between all workers for one cache. */
    private final AtomicBoolean stop;

    /** Cancellation token between all workers for all caches. */
    private final IndexRebuildCancelToken cancelTok;

    /** Index closure. */
    private final SchemaIndexCacheVisitorClosureWrapper wrappedClo;

    /** Partition. */
    private final GridDhtLocalPartition locPart;

    /** Worker future. */
    private final GridFutureAdapter<SchemaIndexCacheStat> fut;

    /** Count of partitions to be processed. */
    private final AtomicInteger partsCnt;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param locPart Partition.
     * @param stop Stop flag between all workers for one cache.
     * @param cancelTok Cancellation token between all workers for all caches.
     * @param clo Index closure.
     * @param fut Worker future.
     * @param partsCnt Count of partitions to be processed.
     */
    public SchemaIndexCachePartitionWorker(
        GridCacheContext cctx,
        GridDhtLocalPartition locPart,
        AtomicBoolean stop,
        IndexRebuildCancelToken cancelTok,
        SchemaIndexCacheVisitorClosure clo,
        GridFutureAdapter<SchemaIndexCacheStat> fut,
        AtomicInteger partsCnt
    ) {
        super(
            cctx.igniteInstanceName(),
            "parallel-idx-worker-" + cctx.cache().name() + "-part-" + locPart.id(),
            cctx.logger(SchemaIndexCachePartitionWorker.class)
        );

        this.cctx = cctx;
        this.locPart = locPart;
        this.cancelTok = cancelTok;

        assert nonNull(stop);
        assert nonNull(clo);
        assert nonNull(fut);
        assert nonNull(partsCnt);

        this.stop = stop;
        wrappedClo = new SchemaIndexCacheVisitorClosureWrapper(clo);
        this.fut = fut;
        this.partsCnt = partsCnt;
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        Throwable err = null;

        try {
            processPartition();
        }
        catch (Throwable e) {
            err = Error.class.isInstance(e) ? new IgniteException(e) : e;

            U.error(log, "Error during create/rebuild index for partition: " + locPart.id(), e);

            stop.set(true);

            int cnt = partsCnt.getAndSet(0);

            if (cnt > 0)
                cctx.group().metrics().addIndexBuildCountPartitionsLeft(-cnt);
        }
        finally {
            fut.onDone(wrappedClo.indexCacheStat, err);
        }
    }

    /**
     * Process partition.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void processPartition() throws IgniteCheckedException {
        if (stop())
            return;

        checkCancelled();

        boolean reserved = false;

        GridDhtPartitionState partState = locPart.state();
        if (partState != EVICTED)
            reserved = (partState == OWNING || partState == MOVING || partState == LOST) && locPart.reserve();

        if (!reserved)
            return;

        try {
            GridCursor<? extends CacheDataRow> cursor = locPart.dataStore().cursor(
                cctx.cacheId(),
                null,
                null,
                KEY_ONLY
            );

            boolean locked = false;

            try {
                int cntr = 0;

                while (!stop() && cursor.next()) {
                    KeyCacheObject key = cursor.get().key();

                    if (!locked) {
                        cctx.shared().database().checkpointReadLock();

                        locked = true;
                    }

                    processKey(key);

                    if (++cntr % batchSize == 0) {
                        cctx.shared().database().checkpointReadUnlock();

                        locked = false;
                    }

                    cctx.cache().metrics0().addIndexRebuildKeyProcessed(1);

                    if (locPart.state() == RENTING)
                        break;
                }

                wrappedClo.addNumberProcessedKeys(cntr);
            }
            finally {
                if (locked)
                    cctx.shared().database().checkpointReadUnlock();
            }
        }
        finally {
            locPart.release();

            if (partsCnt.getAndUpdate(v -> v > 0 ? v - 1 : 0) > 0)
                cctx.group().metrics().decrementIndexBuildCountPartitionsLeft();
        }
    }

    /**
     * Process single key.
     *
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    private void processKey(KeyCacheObject key) throws IgniteCheckedException {
        assert nonNull(key);

        while (!stop()) {
            try {
                checkCancelled();

                GridCacheEntryEx entry = cctx.cache().entryEx(key);

                try {
                    entry.updateIndex(wrappedClo);
                }
                finally {
                    entry.touch();
                }

                break;
            }
            catch (GridDhtInvalidPartitionException ignore) {
                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op.
            }
        }
    }

    /**
     * Check if visit process is not cancelled.
     *
     * @throws IgniteCheckedException If cancelled.
     */
    private void checkCancelled() throws IgniteCheckedException {
        Throwable e = cancelTok.cancelException();

        if (e instanceof SchemaIndexOperationCancellationException)
            throw (SchemaIndexOperationCancellationException)e;
        else if (e != null)
            throw new IgniteCheckedException(e);
    }

    /**
     * Check if index rebuilding needs to be stopped.
     *
     * @return {@code True} if necessary to stop rebuilding indexes.
     */
    private boolean stop() {
        return stop.get() || cctx.kernalContext().isStopping();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexCachePartitionWorker.class, this);
    }

    /**
     * Wrapper class for given closure.
     */
    private class SchemaIndexCacheVisitorClosureWrapper implements SchemaIndexCacheVisitorClosure {
        /** Closure. */
        private final SchemaIndexCacheVisitorClosure clo;

        /** Object for collecting statistics about index update. */
        @Nullable private final SchemaIndexCacheStat indexCacheStat;

        /** */
        private SchemaIndexCacheVisitorClosureWrapper(
            SchemaIndexCacheVisitorClosure clo
        ) {
            this.clo = clo;
            indexCacheStat = getBoolean(IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING, false) ? new SchemaIndexCacheStat() : null;
        }

        /** {@inheritDoc} */
        @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
            if (row != null) {
                clo.apply(row);

                if (indexCacheStat != null) {
                    QueryTypeDescriptorImpl type = cctx.kernalContext().query().typeByValue(
                        cctx.cache().name(),
                        cctx.cacheObjectContext(),
                        row.key(),
                        row.value(),
                        true
                    );

                    if (type != null)
                        indexCacheStat.addType(type);
                }
            }
        }

        /** */
        private void addNumberProcessedKeys(int cnt) {
            if (nonNull(indexCacheStat))
                indexCacheStat.add(cnt);
        }
    }
}
