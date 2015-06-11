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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.*;

/**
 * Reservation mechanism for multiple partitions allowing to do a reservation in one operation.
 */
public class GridDhtPartitionsReservation implements GridReservable {
    /** */
    private final GridCacheContext<?,?> cctx;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final List<GridDhtLocalPartition> parts = new ArrayList<>();

    /** */
    private final AtomicInteger reservations = new AtomicInteger();

    /** */
    private final IgniteInClosure<GridDhtPartitionsReservation> finalize;

    /**
     * @param topVer AffinityTopologyVersion version.
     * @param cctx Cache context.
     * @param finalize Finalizing closure.
     */
    public GridDhtPartitionsReservation(
        AffinityTopologyVersion topVer,
        GridCacheContext<?,?> cctx,
        IgniteInClosure<GridDhtPartitionsReservation> finalize) {
        assert topVer != null;
        assert cctx != null;

        this.topVer = topVer;
        this.cctx = cctx;
        this.finalize = finalize;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Cache context.
     */
    public GridCacheContext<?,?> cacheContext() {
        return cctx;
    }

    /**
     * Registers partition for this group reservation.
     *
     * @param part Partition.
     */
    public void register(GridDhtLocalPartition part) {
        parts.add(part);
    }

    /**
     * Reserves all the registered partitions.
     *
     * @return {@code true} If succeeded.
     */
    @Override public boolean reserve() {
        for (;;) {
            int r = reservations.get();

            if (r == -1) // Invalidated by successful canEvict call.
                return false;

            assert r >= 0 : r;

            if (reservations.compareAndSet(r, r + 1))
                return true;
        }
    }

    /**
     * Releases all the registered partitions.
     */
    @Override public void release() {
        for (;;) {
            int r = reservations.get();

            if (r <= 0)
                throw new IllegalStateException("Method 'reserve' must be called before 'release'.");

            if (reservations.compareAndSet(r, r - 1)) {
                // If it was the last reservation and topology version changed -> attempt to evict partitions.
                if (r == 1 && !topVer.equals(cctx.topology().topologyVersion())) {
                    for (GridDhtLocalPartition part : parts) {
                        if (part.state() == RENTING)
                            part.tryEvictAsync(true);
                    }
                }

                return;
            }
        }
    }

    /**
     * Must be checked in {@link GridDhtLocalPartition#tryEvict(boolean)}.
     * If returns {@code true} then probably partition will be evicted (or at least cleared),
     * so this reservation object becomes invalid and must be dropped from the partition.
     * Also this means that after returning {@code true} here method {@link #reserve()} can not
     * return {@code true} anymore.
     *
     * @return {@code true} If this reservation is NOT reserved and partition CAN be evicted.
     */
    public boolean canEvict() {
        int r = reservations.get();

        assert r >= -1 : r;

        if (r != 0)
            return r == -1;

        if (reservations.compareAndSet(0, -1)) {
            // Remove our self.
            for (GridDhtLocalPartition part : parts)
                part.removeReservation(this);

            if (finalize != null)
                finalize.apply(this);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridDhtPartitionsReservation that = (GridDhtPartitionsReservation)o;

        return topVer.equals(that.topVer) && cctx == that.cctx;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        String cache = cctx.name();

        return 31 * topVer.hashCode() + cache == null ? 0 : cache.hashCode();
    }
}
