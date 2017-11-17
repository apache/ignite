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
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.RENTING;

/**
 * Traversor operating all primary and backup partitions of given cache.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class SchemaIndexCacheVisitorImpl implements SchemaIndexCacheVisitor {
    /** Count of rows, being processed within a single checkpoint lock. */
    private static final int BATCH_SIZE = 1000;

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Row filter. */
    private final SchemaIndexCacheFilter rowFilter;

    /** Cancellation token. */
    private final SchemaIndexOperationCancellationToken cancel;

    /**
     * Constructor.
     *  @param cctx Cache context.
     */
    public SchemaIndexCacheVisitorImpl(GridCacheContext cctx) {
        this(cctx, null, null);
    }

    /**
     * Constructor.
     *  @param cctx Cache context.
     * @param cancel Cancellation token.
     */
    public SchemaIndexCacheVisitorImpl(GridCacheContext cctx, SchemaIndexCacheFilter rowFilter,
        SchemaIndexOperationCancellationToken cancel) {
        this.rowFilter = rowFilter;
        this.cancel = cancel;

        if (cctx.isNear())
            cctx = ((GridNearCacheAdapter)cctx.cache()).dht().context();

        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public void visit(SchemaIndexCacheVisitorClosure clo) throws IgniteCheckedException {
        assert clo != null;

        List<GridDhtLocalPartition> parts = cctx.topology().localPartitions();

        for (int i = 0, size = parts.size(); i < size; i++)
            processPartition(parts.get(i), clo);
    }

    /**
     * Process partition.
     *
     * @param part Partition.
     * @param clo Index closure.
     * @throws IgniteCheckedException If failed.
     */
    private void processPartition(GridDhtLocalPartition part, SchemaIndexCacheVisitorClosure clo)
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

            boolean locked = false;

            try {
                int cntr = 0;

                while (cursor.next()) {
                    KeyCacheObject key = cursor.get().key();

                    if (!locked) {
                        cctx.shared().database().checkpointReadLock();

                        locked = true;
                    }

                    processKey(key, clo);

                    if (++cntr % BATCH_SIZE == 0) {
                        cctx.shared().database().checkpointReadUnlock();

                        locked = false;
                    }

                    if (part.state() == RENTING)
                        break;
                }
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
     * @throws IgniteCheckedException If failed.
     */
    private void processKey(KeyCacheObject key, SchemaIndexCacheVisitorClosure clo) throws IgniteCheckedException {
        while (true) {
            try {
                checkCancelled();

                GridCacheEntryEx entry = cctx.cache().entryEx(key);

                try {
                    entry.updateIndex(rowFilter, clo);
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
        if (cancel != null && cancel.isCancelled())
            throw new IgniteCheckedException("Index creation was cancelled.");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexCacheVisitorImpl.class, this);
    }
}
