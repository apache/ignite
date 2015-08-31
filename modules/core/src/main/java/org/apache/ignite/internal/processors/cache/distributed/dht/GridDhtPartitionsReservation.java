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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.RENTING;

/**
 * Reservation mechanism for multiple partitions allowing to do a reservation in one operation.
 */
public class GridDhtPartitionsReservation implements GridReservable {
    /** */
    private static final GridDhtLocalPartition[] EMPTY = {};

    /** */
    private static final CI1<GridDhtPartitionsReservation> NO_OP = new CI1<GridDhtPartitionsReservation>() {
        @Override public void apply(GridDhtPartitionsReservation gridDhtPartitionsReservation) {
            throw new IllegalStateException();
        }
    };

    /** */
    private final Object appKey;

    /** */
    private final GridCacheContext<?,?> cctx;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final AtomicReference<GridDhtLocalPartition[]> parts = new AtomicReference<>();

    /** */
    private final AtomicReference<CI1<GridDhtPartitionsReservation>> unpublish = new AtomicReference<>();

    /** */
    private final AtomicInteger reservations = new AtomicInteger();

    /**
     * @param topVer AffinityTopologyVersion version.
     * @param cctx Cache context.
     * @param appKey Application key for reservation.
     */
    public GridDhtPartitionsReservation(AffinityTopologyVersion topVer, GridCacheContext<?,?> cctx, Object appKey) {
        assert topVer != null;
        assert cctx != null;
        assert appKey != null;

        this.topVer = topVer;
        this.cctx = cctx;
        this.appKey = appKey;
    }

    /**
     * Registers all the given partitions for this reservation.
     *
     * @param parts Partitions.
     * @return {@code true} If registration succeeded and this reservation can be published.
     */
    public boolean register(Collection<? extends GridReservable> parts) {
        assert !F.isEmpty(parts) : "empty partitions list";

        GridDhtLocalPartition[] arr = new GridDhtLocalPartition[parts.size()];

        int i = 0;
        int prevPart = -1;
        boolean sorted = true; // Most probably it is a sorted list.

        for (GridReservable part : parts) {
            arr[i] = (GridDhtLocalPartition)part;

            if (sorted) { // Make sure it will be a sorted array.
                int id = arr[i].id();

                if (id <= prevPart)
                    sorted = false;

                prevPart = id;
            }

            i++;
        }

        if (!sorted)
            Arrays.sort(arr);

        i = 0;
        prevPart = -1;

        // Register in correct sort order.
        for (GridDhtLocalPartition part : arr) {
            if (prevPart == part.id())
                throw new IllegalStateException("Duplicated partitions.");

            prevPart = part.id();

            if (!part.addReservation(this)) {
                if (i != 0)
                    throw new IllegalStateException(
                        "Trying to reserve different sets of partitions for the same topology version.");

                return false;
            }

            i++;
        }

        if (!this.parts.compareAndSet(null, arr))
            throw new IllegalStateException("Partitions can be registered only once.");

        assert reservations.get() != -1 : "all the partitions must be reserved before register, we can't be invalidated";

        return true;
    }

    /**
     * Must be called when this reservation is published.
     *
     * @param unpublish Closure to unpublish this reservation when it will become invalid.
     */
    public void onPublish(CI1<GridDhtPartitionsReservation> unpublish) {
        assert unpublish != null;

        if (!this.unpublish.compareAndSet(null, unpublish))
            throw new IllegalStateException("Unpublishing closure can be set only once.");

        if (reservations.get() == -1)
            unregister();
    }

    /**
     * Reserves all the registered partitions.
     *
     * @return {@code true} If succeeded.
     */
    @Override public boolean reserve() {
        assert parts.get() != null : "partitions must be registered before the first reserve attempt";

        for (;;) {
            int r = reservations.get();

            if (r == -1) // Invalidated.
                return false;

            assert r >= 0 : r;

            if (reservations.compareAndSet(r, r + 1))
                return true;
        }
    }

    /**
     * @param parts Partitions.
     */
    private static void tryEvict(GridDhtLocalPartition[] parts) {
        if (parts == null)  // Can be not initialized yet.
            return ;

        for (GridDhtLocalPartition part : parts)
            tryEvict(part);
    }

    /**
     * @param part Partition.
     */
    private static void tryEvict(GridDhtLocalPartition part) {
        if (part.state() == RENTING && part.reservations() == 0)
            part.tryEvictAsync(true);
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
                if (r == 1 && !topVer.equals(cctx.topology().topologyVersion()))
                    tryEvict(parts.get());

                return;
            }
        }
    }

    /**
     * Unregisters from all the partitions and unpublishes this reservation.
     */
    private void unregister() {
        GridDhtLocalPartition[] arr = parts.get();

        // Unregister from partitions.
        if (!F.isEmpty(arr) && parts.compareAndSet(arr, EMPTY)) {
            // Reverse order makes sure that addReservation on the same topVer
            // reservation will fail on the first partition.
            for (int i = arr.length - 1; i >= 0; i--) {
                GridDhtLocalPartition part = arr[i];

                part.removeReservation(this);

                tryEvict(part);
            }
        }

        // Unpublish.
        CI1<GridDhtPartitionsReservation> u = unpublish.get();

        if (u != null && u != NO_OP && unpublish.compareAndSet(u, NO_OP))
            u.apply(this);
    }

    /**
     * Must be checked in {@link GridDhtLocalPartition#tryEvict(boolean)}.
     * If returns {@code true} this reservation object becomes invalid and partitions
     * can be evicted or at least cleared.
     * Also this means that after returning {@code true} here method {@link #reserve()} can not
     * return {@code true} anymore.
     *
     * @return {@code true} If this reservation was successfully invalidated because it was not
     *          reserved and partitions can be evicted.
     */
    public boolean invalidate() {
        assert parts.get() != null : "all parts must be reserved before registration";

        int r = reservations.get();

        assert r >= -1 : r;

        if (r != 0)
            return r == -1;

        if (reservations.compareAndSet(0, -1)) {
            unregister();

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

        return cctx == that.cctx && topVer.equals(that.topVer) && appKey.equals(that.appKey);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        String name = cctx.name();

        int result = name == null ? 0 : name.hashCode();

        result = 31 * result + appKey.hashCode();
        result = 31 * result + topVer.hashCode();

        return result;
    }
}