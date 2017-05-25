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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.Collection;

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

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param cancel Cancellation token.
     */
    public SchemaIndexCacheVisitorImpl(GridQueryProcessor qryProc, GridCacheContext cctx, String cacheName,
        String tblName, SchemaIndexOperationCancellationToken cancel) {
        this.qryProc = qryProc;
        this.cacheName = cacheName;
        this.tblName = tblName;
        this.cancel = cancel;

        if (cctx.isNear())
            cctx = ((GridNearCacheAdapter)cctx.cache()).dht().context();

        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public void visit(SchemaIndexCacheVisitorClosure clo) throws IgniteCheckedException {
        assert clo != null;

        FilteringVisitorClosure filterClo = new FilteringVisitorClosure(clo);

        Collection<GridDhtLocalPartition> parts = cctx.topology().localPartitions();

        for (GridDhtLocalPartition part : parts)
            processPartition(part, filterClo);
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
            GridCursor<? extends CacheDataRow> cursor = part.dataStore().cursor();

            while (cursor.next()) {
                CacheDataRow row = cursor.get();

                KeyCacheObject key = row.key();

                processKey(key, row.link(), clo);
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
     * @param link Link.
     * @param clo Closure.
     * @throws IgniteCheckedException If failed.
     */
    private void processKey(KeyCacheObject key, long link, FilteringVisitorClosure clo) throws IgniteCheckedException {
        while (true) {
            try {
                checkCancelled();

                GridCacheEntryEx entry = cctx.cache().entryEx(key);

                try {
                    entry.updateIndex(clo, link);
                }
                finally {
                    cctx.evicts().touch(entry, AffinityTopologyVersion.NONE);
                }

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
        public FilteringVisitorClosure(SchemaIndexCacheVisitorClosure target) {
            this.target = target;
        }

        /** {@inheritDoc} */
        @Override public void apply(KeyCacheObject key, int part, CacheObject val, GridCacheVersion ver,
            long expiration, long link) throws IgniteCheckedException {
            if (qryProc.belongsToTable(cctx, cacheName, tblName, key, val))
                target.apply(key, part, val, ver, expiration, link);
        }
    }
}
