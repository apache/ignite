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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.PartitionReservation;

/** */
public abstract class AbstractCacheScan<Row> implements Iterable<Row>, AutoCloseable {
    /** */
    protected final GridCacheContext<?, ?> cctx;

    /** */
    protected final ExecutionContext<Row> ectx;

    /** */
    protected final AffinityTopologyVersion topVer;

    /** */
    protected final int[] parts;

    /** */
    protected final boolean explicitParts;

    /** */
    private PartitionReservation reservation;

    /** */
    protected volatile List<GridDhtLocalPartition> reservedParts;

    /** */
    AbstractCacheScan(ExecutionContext<Row> ectx, GridCacheContext<?, ?> cctx, int[] parts) {
        this.ectx = ectx;
        this.cctx = cctx;

        topVer = ectx.topologyVersion();

        explicitParts = parts != null;

        if (cctx.isReplicated())
            this.parts = IntStream.range(0, cctx.affinity().partitions()).toArray();
        else {
            if (parts != null)
                this.parts = parts;
            else {
                Collection<Integer> primaryParts = cctx.affinity().primaryPartitions(
                    cctx.kernalContext().localNodeId(), topVer);

                this.parts = primaryParts.stream().mapToInt(Integer::intValue).toArray();
            }
        }
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
        if (reservation != null)
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

        try {
            PartitionReservation reservation;

            try {
                reservation = cctx.kernalContext().query().partitionReservationManager().reservePartitions(
                    cctx, topVer, explicitParts ? parts : null, ectx.originatingNodeId(), "qryId=" + ectx.queryId());
            }
            catch (IgniteCheckedException e) {
                throw new ClusterTopologyException("Failed to reserve partition for query execution", e);
            }

            if (reservation.failed()) {
                reservation.release();

                throw new ClusterTopologyException(reservation.error());
            }

            this.reservation = reservation;

            List<GridDhtLocalPartition> reservedParts = new ArrayList<>(parts.length);

            for (int i = 0; i < parts.length; i++)
                reservedParts.add(top.localPartition(parts[i]));

            this.reservedParts = reservedParts;
        }
        finally {
            top.readUnlock();
        }
    }

    /** */
    private synchronized void release() {
        if (reservation != null)
            reservation.release();

        reservation = null;
    }
}
