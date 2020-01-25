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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class TableScan implements Iterable<Object[]> {
    /** */
    private final ExecutionContext ectx;

    /** */
    private final TableDescriptor desc;

    /** */
    public TableScan(ExecutionContext ectx, TableDescriptor desc) {
        this.ectx = ectx;
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Object[]> iterator() {
        try {
            return new IteratorImpl().init();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Table scan iterator.
     */
    public class IteratorImpl extends GridCloseableIteratorAdapter<Object[]> {
        /** */
        private int cacheId;

        /** */
        private Queue<GridDhtLocalPartition> parts;

        /** */
        private GridDhtLocalPartition part;

        /** */
        private GridCursor<? extends CacheDataRow> cur;

        /** */
        private Object[] next;

        /** {@inheritDoc} */
        @Override protected Object[] onNext() {
            if (next == null)
                throw new NoSuchElementException();

            Object[] next = this.next;

            this.next = null;

            return next;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            assert parts != null;

            if (next != null)
                return true;

            while (true) {
                if (cur == null) {
                    if ((part = parts.poll()) == null)
                        break;

                    cur = part.dataStore().cursor(cacheId, ectx.mvccSnapshot());
                }

                if (cur.next()) {
                    CacheDataRow row = cur.get();

                    if (!desc.match(row))
                        continue;

                    next = desc.toRow(ectx, row);

                    break;
                } else {
                    cur = null;

                    part.release();
                    part = null;
                }
            }

            return next != null;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() {
            if (part != null)
                part.release();

            part = null;

            while (!F.isEmpty(parts))
                parts.poll().release();

            parts = null;
        }

        /** */
        public Iterator<Object[]> init() throws IgniteCheckedException {
            if (isClosed())
                return Collections.emptyIterator();

            GridCacheContext<?, ?> cctx = desc.cacheContext();

            if (!cctx.gate().enterIfNotStopped()) {
                close();

                throw new CacheStoppedException(cctx.name());
            }

            try {
                GridDhtPartitionTopology top = cctx.topology();

                top.readLock();
                try {
                    GridDhtTopologyFuture fut = top.topologyVersionFuture();
                    AffinityTopologyVersion topVer = ectx.parent().topologyVersion();

                    if (!fut.isDone() || fut.topologyVersion().compareTo(topVer) != 0)
                        throw new ClusterTopologyCheckedException("Failed to execute query. Retry on stable topology.");

                    if (cctx.isPartitioned())
                        reservePartitioned(top);
                    else
                        reserveReplicated(top);
                }
                finally {
                    top.readUnlock();
                }

                cacheId = cctx.cacheId();

                return this;
            }
            catch (Exception e) {
                Commons.close(this, e);

                throw e;
            }
            finally {
                cctx.gate().leave();
            }
        }

        /** */
        private void reserveReplicated(GridDhtPartitionTopology top) {
            List<GridDhtLocalPartition> localParts = top.localPartitions();

            parts = new ArrayDeque<>(localParts);

            for (GridDhtLocalPartition local : localParts) {
                if (!local.reserve())
                    throw reservationException();
                else if (local.state() != GridDhtPartitionState.OWNING) {
                    local.release();

                    throw reservationException();
                }

                parts.offer(local);
            }
        }

        /** */
        private void reservePartitioned(GridDhtPartitionTopology top) {
            AffinityTopologyVersion topVer = ectx.parent().topologyVersion();
            int[] partitions = ectx.partitions();

            assert topVer != null && !F.isEmpty(partitions);

            parts = new ArrayDeque<>(partitions.length);

            for (int p : partitions) {
                GridDhtLocalPartition local = top.localPartition(p, topVer, false);

                if (local == null || !local.reserve())
                    throw reservationException();
                else if (local.state() != GridDhtPartitionState.OWNING) {
                    local.release();

                    throw reservationException();
                }

                parts.offer(local);
            }
        }

        /** */
        private IgniteSQLException reservationException() {
            return new IgniteSQLException("Failed to reserve partition for query execution. Retry on stable topology.");
        }
    }
}
