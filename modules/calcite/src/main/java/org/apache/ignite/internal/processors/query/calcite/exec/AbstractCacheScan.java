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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** */
public abstract class AbstractCacheScan<Row> implements Iterable<Row>, AutoCloseable {
    /** */
    protected final GridCacheContext<?, ?> cctx;

    /** */
    protected final ExecutionContext<Row> ectx;

    /** */
    protected final CacheTableDescriptor desc;

    /** */
    protected final RowFactory<Row> factory;

    /** */
    protected final RelDataType rowType;

    /** */
    protected final AffinityTopologyVersion topVer;

    /** */
    protected final int[] parts;

    /** */
    protected volatile List<GridDhtLocalPartition> reserved;

    /** Participating colunms. */
    protected final ImmutableBitSet requiredColumns;

    /** */
    AbstractCacheScan(
        ExecutionContext<Row> ectx,
        CacheTableDescriptor desc,
        int[] parts,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        this.ectx = ectx;
        cctx = desc.cacheContext();
        this.desc = desc;
        this.parts = parts;
        this.requiredColumns = requiredColumns;

        rowType = desc.rowType(ectx.getTypeFactory(), requiredColumns);
        factory = ectx.rowHandler().factory(ectx.getTypeFactory(), rowType);
        topVer = ectx.topologyVersion();
    }

    /** {@inheritDoc} */
    @Override public final Iterator<Row> iterator() {
        reserve();

        try {
            return createIterator();
        }
        catch (Exception e) {
            release();

            throw e;
        }
    }

    /** Rows iterator.*/
    protected abstract Iterator<Row> createIterator();

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
}
