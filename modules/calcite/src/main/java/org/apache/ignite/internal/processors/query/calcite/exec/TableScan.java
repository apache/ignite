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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** */
public class TableScan<Row> implements Iterable<Row>, AutoCloseable {
    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final Predicate<Row> filters;

    /** */
    private final ExecutionContext<Row> ectx;

    /** */
    private final CacheTableDescriptor desc;

    /** */
    private final RowFactory<Row> factory;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final int[] parts;

    /** */
    private final MvccSnapshot mvccSnapshot;

    /** */
    private volatile List<GridDhtLocalPartition> reserved;

    /** */
    private final Function<Row, Row> rowTransformer;

    /** Participating colunms. */
    private final ImmutableBitSet requiredColunms;

    /** */
    public TableScan(
        ExecutionContext<Row> ectx,
        CacheTableDescriptor desc,
        int[] parts,
        Predicate<Row> filters,
        Function<Row, Row> rowTransformer,
        @Nullable ImmutableBitSet requiredColunms
    ) {
        this.ectx = ectx;
        cctx = desc.cacheContext();
        this.desc = desc;
        this.parts = parts;
        this.filters = filters;
        this.rowTransformer = rowTransformer;
        this.requiredColunms = requiredColunms;

        RelDataType rowType = desc.rowType(this.ectx.getTypeFactory(), requiredColunms);

        factory = this.ectx.rowHandler().factory(this.ectx.getTypeFactory(), rowType);
        topVer = ectx.topologyVersion();
        mvccSnapshot = ectx.mvccSnapshot();
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> iterator() {
        reserve();
        try {
            return new IteratorImpl();
        }
        catch (Exception e) {
            release();

            throw e;
        }
    }

    /** */
    @Override public void close() {
        release();
    }

    /** */
    private synchronized void reserve() {
        if (reserved != null)
            return;

        GridDhtPartitionTopology top = cctx.topology();
        top.readLock();

        GridDhtTopologyFuture topFut = top.topologyVersionFuture();

        boolean done = topFut.isDone();

        if (!done || !(topFut.topologyVersion().compareTo(topVer) >= 0
            && cctx.shared().exchange().lastAffinityChangedTopologyVersion(topFut.initialVersion()).compareTo(topVer) <= 0)) {
            top.readUnlock();

            throw new ClusterTopologyException("Topology was changed. Please retry on stable topology.");
        }

        List<GridDhtLocalPartition> toReserve;
        if (cctx.isReplicated()) {
            int partsCnt = cctx.affinity().partitions();
            toReserve = new ArrayList<>(partsCnt);
            for (int i = 0; i < partsCnt; i++)
                toReserve.add(top.localPartition(i));
        }
        else if (cctx.isPartitioned()) {
            assert parts != null;

            toReserve = new ArrayList<>(parts.length);
            for (int i = 0; i < parts.length; i++)
                toReserve.add(top.localPartition(parts[i]));
        }
        else
            toReserve = Collections.emptyList();

        reserved = new ArrayList<>(toReserve.size());

        try {
            for (GridDhtLocalPartition part : toReserve) {
                if (part == null || !part.reserve())
                    throw new ClusterTopologyException("Failed to reserve partition for query execution. Retry on stable topology.");
                else if (part.state() != GridDhtPartitionState.OWNING) {
                    part.release();

                    throw new ClusterTopologyException("Failed to reserve partition for query execution. Retry on stable topology.");
                }

                reserved.add(part);
            }
        }
        catch (Exception e) {
            release();

            throw e;
        }
        finally {
            top.readUnlock();
        }
    }

    /** */
    private synchronized void release() {
        if (F.isEmpty(reserved))
            return;

        reserved.forEach(GridDhtLocalPartition::release);

        reserved = null;
    }

    /**
     * Table scan iterator.
     */
    private class IteratorImpl extends GridIteratorAdapter<Row> {
        /** */
        private final Queue<GridDhtLocalPartition> parts;

        /** */
        private GridCursor<? extends CacheDataRow> cur;

        /** */
        private Row next;

        /** */
        private IteratorImpl() {
            assert reserved != null;

            parts = new ArrayDeque<>(reserved);
        }

        /** {@inheritDoc} */
        @Override public boolean hasNextX() throws IgniteCheckedException {
            advance();

            return next != null;
        }

        /** {@inheritDoc} */
        @Override public Row nextX() throws IgniteCheckedException {
            advance();

            if (next == null)
                throw new NoSuchElementException();

            Row next = this.next;

            this.next = null;

            return next;
        }

        /** {@inheritDoc} */
        @Override public void removeX() {
            throw new UnsupportedOperationException("Remove is not supported.");
        }

        /** */
        private void advance() throws IgniteCheckedException {
            assert parts != null;

            if (next != null)
                return;

            while (true) {
                if (cur == null) {
                    GridDhtLocalPartition part = parts.poll();
                    if (part == null)
                        break;

                    cur = part.dataStore().cursor(cctx.cacheId(), mvccSnapshot);
                }

                if (cur.next()) {
                    CacheDataRow row = cur.get();

                    if (!desc.match(row))
                        continue;

                    Row r = desc.toRow(ectx, row, factory, requiredColunms);

                    if (filters != null && !filters.test(r))
                        continue;

                    if (rowTransformer != null)
                        r = rowTransformer.apply(r);

                    next = r;
                    break;
                }
                else
                    cur = null;
            }
        }
    }
}
