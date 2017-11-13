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

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.RENTING;

/**
 * Traversor operating all primary and backup partitions of given cache.
 */
public class SchemaIndexCacheVisitorImpl implements SchemaIndexCacheVisitor {
    /** Query procssor. */
    private final GridQueryProcessor qryProc;

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Cache name. */
    private final String cacheName;

    /** Table name. */
    private final String tblName;

    /** Cancellation token. */
    private final SchemaIndexOperationCancellationToken cancel;

    /** Index creation parallelism level. */
    private int parallel;

    /** Parallel index creation workers interruption flag. */
    private volatile boolean failed;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param cancel Cancellation token.
     * @param parallel Index creation parallelism level.
     */
    public SchemaIndexCacheVisitorImpl(GridQueryProcessor qryProc, GridCacheContext cctx, String cacheName,
        String tblName, SchemaIndexOperationCancellationToken cancel, int parallel) {

        this.qryProc = qryProc;
        this.cacheName = cacheName;
        this.tblName = tblName;
        this.cancel = cancel;

        int cores = Runtime.getRuntime().availableProcessors();

        if (parallel > 0)
            this.parallel = Math.min(cores, parallel);
        else if (parallel == 0)
            this.parallel =  Math.max(1, cores / 4); // Default value = 25% of available cores

        if (cctx.isNear())
            cctx = ((GridNearCacheAdapter)cctx.cache()).dht().context();

        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public void visit(SchemaIndexCacheVisitorClosure clo) throws IgniteCheckedException {
        assert clo != null;
        assert parallel > 0;

        FilteringVisitorClosure filterClo = new FilteringVisitorClosure(clo);

        List<GridDhtLocalPartition> parts = cctx.topology().localPartitions();

        if (parts.isEmpty())
            return;

        if (parallel == 1) {
            for (GridDhtLocalPartition part : parts)
                processPartition(part, filterClo);
        }
        else
            processPartitionsConcurrently(parts, filterClo);
    }

    /**
     * Processes partitions list concurrently.
     *
     * @param parts Local cache partitions list.
     * @param filterClo Closure to be applied to the cache entries (rows) for index creating.
     * @throws IgniteCheckedException If failed.
     */
    private void processPartitionsConcurrently(final List<GridDhtLocalPartition> parts,
        final FilteringVisitorClosure filterClo) throws IgniteCheckedException {

        GridCompoundFuture<Void, Void> compFut = new GridCompoundFuture<>();

        for (int i = 0; i < parallel; i++) {

            GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

            compFut.add(fut);

            PartitionsIndexingWorker worker = new PartitionsIndexingWorker(fut, parts, i, filterClo);

            if (i == parallel - 1)  // Last chunk of partitions is treated by the current thread.
                worker.run();
            else
                new Thread(worker).start();
        }

        compFut.markInitialized();

        compFut.get();
    }

    /**
     * Process partition.
     *
     * @param part Partition.
     * @param clo Index closure.
     * @throws IgniteCheckedException If failed.
     */
    private void processPartition(GridDhtLocalPartition part, FilteringVisitorClosure clo)
        throws IgniteCheckedException {
        checkCancelled();

        boolean reserved = false;

        if (part != null && part.state() != EVICTED)
            reserved = (part.state() == OWNING || part.state() == RENTING) && part.reserve();

        if (!reserved)
            return;

        try {
            GridCursor<? extends CacheDataRow> cursor = part.dataStore().cursor(cctx.cacheId(),
                null,
                null,
                CacheDataRowAdapter.RowData.KEY_ONLY);

            while (cursor.next() && !failed) {
                CacheDataRow row = cursor.get();

                KeyCacheObject key = row.key();

                processKey(key, clo);

                if (part.state() == RENTING)
                    break;
            }
        }
        finally {
            part.release();
        }
    }

    /**
     * Process single key.
     *
     * @param key Key.
     * @param clo Closure.
     * @throws IgniteCheckedException If failed.
     */
    private void processKey(KeyCacheObject key, FilteringVisitorClosure clo) throws IgniteCheckedException {
        while (true) {
            try {
                checkCancelled();

                GridCacheEntryEx entry = cctx.cache().entryEx(key);

                try {
                    entry.updateIndex(clo);
                }
                finally {
                    cctx.evicts().touch(entry, AffinityTopologyVersion.NONE);
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
        if (cancel.isCancelled())
            throw new IgniteCheckedException("Index creation was cancelled.");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexCacheVisitorImpl.class, this);
    }

    /**
     * Filtering visitor closure.
     */
    private class FilteringVisitorClosure implements SchemaIndexCacheVisitorClosure {

        /** Target closure. */
        private final SchemaIndexCacheVisitorClosure target;

        /**
         * Constructor.
         *
         * @param target Target.
         */
        FilteringVisitorClosure(SchemaIndexCacheVisitorClosure target) {
            this.target = target;
        }

        /** {@inheritDoc} */
        @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
            if (qryProc.belongsToTable(cctx, cacheName, tblName, row.key(), row.value()))
                target.apply(row);
        }
    }

    /**
     * Partitions index update worker.
     */
    private class PartitionsIndexingWorker extends GridWorker {

        /** Partition number remainder of division to be indexed by this worker. */
        private final int remainder;

        /** Indexing closure. */
        private final FilteringVisitorClosure filterClo;

        /** Processing result future. */
        private final GridFutureAdapter<Void> fut;

        /** Cache partitions to be indexed. */
        private final List<GridDhtLocalPartition> parts;

        /**
         * Constructor.
         *
         * @param fut Processing result future.
         * @param parts Cache partitions to be indexed.
         * @param remainder Partition number remainder of division to be indexed by this worker.
         * @param filterClo Indexing closure.
         */
        private PartitionsIndexingWorker(final GridFutureAdapter<Void> fut,
            final List<GridDhtLocalPartition> parts, int remainder, FilteringVisitorClosure filterClo) {
            super(cctx.igniteInstanceName(), "parallel-idx-creation-worker-" + cacheName + "-" + tblName + "-"
                    + remainder, cctx.logger(SchemaIndexCacheVisitorImpl.class.getName()));

            this.remainder = remainder;
            this.filterClo = filterClo;
            this.parts = parts;
            this.fut = fut;
        }

        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                for (int i = 0; i < parts.size(); i++) {
                    if (failed)
                        break;

                    if (i % parallel == remainder) {
                        GridDhtLocalPartition part = parts.get(i);

                        processPartition(part, filterClo);
                    }
                }
            }
            catch (Throwable e) {
                err = e;

                U.error(log, "Error during parallel index creation on thread " + name(), e);

                failed = true;
            }
            finally {
                fut.onDone(err);
            }
        }
    }
}
