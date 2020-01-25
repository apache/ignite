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
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

/**
 * Traversor operating all primary and backup partitions of given cache.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class SchemaIndexCacheVisitorImpl implements SchemaIndexCacheVisitor {
    /** Default degree of parallelism. */
    private static final int DFLT_PARALLELISM =
        Math.min(4, Math.max(1, Runtime.getRuntime().availableProcessors() / 4));

    /** Count of rows, being processed within a single checkpoint lock. */
    private static final int BATCH_SIZE = 1000;

    //TODO: field is not final for testability. This should be fixed.
    /** Is extra index rebuild logging enabled. */
    private static boolean IS_EXTRA_INDEX_REBUILD_LOGGING_ENABLED =
        IgniteSystemProperties.getBoolean(IGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING, false);

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Row filter. */
    private final SchemaIndexCacheFilter rowFilter;

    /** Cancellation token. */
    private final SchemaIndexOperationCancellationToken cancel;

    /** Parallelism. */
    private final int parallelism;

    /** Whether to stop the process. */
    private volatile boolean stop;

    /** Logger. */
    private IgniteLogger log;

    /**
     * Constructor.
     *  @param cctx Cache context.
     */
    public SchemaIndexCacheVisitorImpl(GridCacheContext cctx) {
        this(cctx, null, null, 0);
    }

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param rowFilter Row filter.
     * @param cancel Cancellation token.
     * @param parallelism Degree of parallelism.
     */
    public SchemaIndexCacheVisitorImpl(GridCacheContext cctx, SchemaIndexCacheFilter rowFilter,
        SchemaIndexOperationCancellationToken cancel, int parallelism) {
        this.rowFilter = rowFilter;
        this.cancel = cancel;

        if (parallelism > 0)
            this.parallelism = Math.min(Runtime.getRuntime().availableProcessors(), parallelism);
        else
            this.parallelism =  DFLT_PARALLELISM;

        if (cctx.isNear())
            cctx = ((GridNearCacheAdapter)cctx.cache()).dht().context();

        this.cctx = cctx;

        this.log = cctx.logger(SchemaIndexCacheVisitorImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void visit(SchemaIndexCacheVisitorClosure clo) throws IgniteCheckedException {
        assert clo != null;

        List<GridDhtLocalPartition> parts = cctx.topology().localPartitions();

        if (parts.isEmpty())
            return;

        GridCompoundFuture<SchemaIndexCacheStat, SchemaIndexCacheStat> fut = null;

        if (parallelism > 1) {
            fut = new GridCompoundFuture<>(new IgniteReducer<SchemaIndexCacheStat, SchemaIndexCacheStat>() {
                private final SchemaIndexCacheStat res = new SchemaIndexCacheStat();

                @Override public boolean collect(SchemaIndexCacheStat msg) {
                    synchronized (res) {
                        if (msg != null) {
                            res.scanned += msg.scanned;
                            res.types.addAll(msg.types);
                        }
                    }

                    return true;
                }

                @Override public SchemaIndexCacheStat reduce() {
                    synchronized (res) {
                        return res;
                    }
                }
            });

            for (int i = 1; i < parallelism; i++)
                fut.add(processPartitionsAsync(parts, clo, i));

            fut.markInitialized();
        }

        final SchemaIndexCacheStat stat0 = processPartitions(parts, clo, 0);

        if (fut != null) {
            final SchemaIndexCacheStat st = fut.get();

            stat0.scanned += st.scanned;
            stat0.types.addAll(st.types);
        }

        printIndexStats(stat0);
    }

    /**
     * Prints index cache stats to log.
     *
     * @param stat Index cache stats.
     * @throws IgniteCheckedException if failed to get index size.
     */
    private void printIndexStats(SchemaIndexCacheStat stat) throws IgniteCheckedException {
        if (!IS_EXTRA_INDEX_REBUILD_LOGGING_ENABLED)
            return;

        StringBuilder res = new StringBuilder();

        res.append("Details for cache rebuilding [name=" + cctx.cache().name()
            + ", grpName=" + cctx.group().name() + ']');
        res.append(U.nl());
        res.append("   Scanned rows " + stat.scanned + ", visited types " + stat.types);
        res.append(U.nl());

        final GridQueryIndexing idx = cctx.kernalContext().query().getIndexing();

        for (String type0 : stat.types) {
            final QueryTypeDescriptorImpl type = cctx.kernalContext().query().typeByName(type0);

            res.append("        Type name=" + type.name());
            res.append(U.nl());

            String pk = "_key_PK";

            res.append("            Index: name=" + pk + ", size=" + idx.indexSize(type.schemaName(), pk));
            res.append(U.nl());

            final Map<String, GridQueryIndexDescriptor> indexes = type.indexes();

            for (GridQueryIndexDescriptor descriptor : indexes.values()) {
                final long size = idx.indexSize(type.schemaName(), descriptor.name());

                res.append("            Index: name=" + descriptor.name() + ", size=" + size);
                res.append(U.nl());
            }
        }

        log.info(res.toString());
    }

    /**
     * Process partitions asynchronously.
     *
     * @param parts Partitions.
     * @param clo Closure.
     * @param remainder Remainder.
     * @return Future.
     */
    private GridFutureAdapter<SchemaIndexCacheStat> processPartitionsAsync(List<GridDhtLocalPartition> parts,
        SchemaIndexCacheVisitorClosure clo, int remainder) {
        GridFutureAdapter<SchemaIndexCacheStat> fut = new GridFutureAdapter<>();

        AsyncWorker worker = new AsyncWorker(parts, clo, remainder, fut);

        new IgniteThread(worker).start();

        return fut;
    }

    /**
     * Process partitions.
     *
     * @param parts Partitions.
     * @param clo Closure.
     * @param remainder Remainder.
     * @return Index statistics.
     * @throws IgniteCheckedException If failed.
     */
    private SchemaIndexCacheStat processPartitions(List<GridDhtLocalPartition> parts, SchemaIndexCacheVisitorClosure clo,
        int remainder)
        throws IgniteCheckedException {

        SchemaIndexCacheStat tmp = new SchemaIndexCacheStat();

        for (int i = 0, size = parts.size(); i < size; i++) {
            if (stop)
                break;

            if ((i % parallelism) == remainder)
                processPartition(parts.get(i), clo, tmp);
        }

        return tmp;
    }

    /**
     * Process partition.
     *
     * @param part Partition.
     * @param clo Index closure.
     * @param res String builder to accumulate details.
     * @throws IgniteCheckedException If failed.
     */
    private void processPartition(GridDhtLocalPartition part, SchemaIndexCacheVisitorClosure clo, SchemaIndexCacheStat res)
        throws IgniteCheckedException {
        checkCancelled();

        boolean reserved = false;

        if (part != null && part.state() != EVICTED)
            reserved = (part.state() == OWNING || part.state() == RENTING) && part.reserve();

        if (!reserved)
            return;

        try {
            GridCursor<? extends CacheDataRow> cursor = part.dataStore().cursor(cctx.cacheId(), null, null,
                CacheDataRowAdapter.RowData.KEY_ONLY);

            boolean locked = false;

            try {
                int cntr = 0;

                while (cursor.next() && !stop) {
                    KeyCacheObject key = cursor.get().key();

                    if (!locked) {
                        cctx.shared().database().checkpointReadLock();

                        locked = true;
                    }

                    final GridQueryTypeDescriptor type = processKey(key, clo);

                    if (type != null)
                        res.types.add(type.name());

                    if (++cntr % BATCH_SIZE == 0) {
                        cctx.shared().database().checkpointReadUnlock();

                        locked = false;
                    }

                    if (part.state() == RENTING)
                        break;
                }

                res.scanned += cntr;
            }
            finally {
                if (locked)
                    cctx.shared().database().checkpointReadUnlock();
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
     * @return Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private GridQueryTypeDescriptor processKey(KeyCacheObject key, SchemaIndexCacheVisitorClosure clo) throws IgniteCheckedException {
        while (true) {
            try {
                checkCancelled();

                GridCacheEntryEx entry = cctx.cache().entryEx(key);

                try {
                    return entry.updateIndex(rowFilter, clo);
                }
                finally {
                    entry.touch();
                }
            }
            catch (GridDhtInvalidPartitionException ignore) {
                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op.
            }
        }

        return null;
    }

    /**
     * Check if visit process is not cancelled.
     *
     * @throws IgniteCheckedException If cancelled.
     */
    private void checkCancelled() throws IgniteCheckedException {
        if (cancel != null && cancel.isCancelled())
            throw new IgniteCheckedException("Index creation was cancelled.");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexCacheVisitorImpl.class, this);
    }

    /**
     * Async worker.
     */
    private class AsyncWorker extends GridWorker {
        /** Partitions. */
        private final List<GridDhtLocalPartition> parts;

        /** Closure. */
        private final SchemaIndexCacheVisitorClosure clo;

        /** Remained.. */
        private final int remainder;

        /** Future. */
        private final GridFutureAdapter<SchemaIndexCacheStat> fut;

        /**
         * Constructor.
         *
         * @param parts Partitions.
         * @param clo Closure.
         * @param remainder Remainder.
         * @param fut Future.
         */
        @SuppressWarnings("unchecked")
        public AsyncWorker(List<GridDhtLocalPartition> parts, SchemaIndexCacheVisitorClosure clo, int remainder,
            GridFutureAdapter<SchemaIndexCacheStat> fut) {
            super(cctx.igniteInstanceName(), "parallel-idx-worker-" + cctx.cache().name() + "-" + remainder,
                cctx.logger(AsyncWorker.class));

            this.parts = parts;
            this.clo = clo;
            this.remainder = remainder;
            this.fut = fut;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                processPartitions(parts, clo, remainder);
            }
            catch (Throwable e) {
                err = e;

                U.error(log, "Error during parallel index create/rebuild.", e);

                stop = true;
            }
            finally {
                fut.onDone(err);
            }
        }
    }
}
