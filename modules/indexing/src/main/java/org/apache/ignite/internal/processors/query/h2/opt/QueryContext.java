/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.query.h2.opt.join.DistributedJoinContext;
import org.apache.ignite.internal.processors.query.h2.twostep.PartitionReservation;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.Nullable;

/**
 * Thread local SQL query context which is intended to be accessible from everywhere.
 */
public class QueryContext {
    /** Segment ID. */
    private final int segment;

    /** */
    private final IndexingQueryFilter filter;

    /** Distributed join context. */
    private final DistributedJoinContext distributedJoinCtx;

    /** */
    private final MvccSnapshot mvccSnapshot;

    /** */
    private final PartitionReservation reservations;

    /**
     * Constructor.
     *
     * @param segment Index segment ID.
     * @param filter Filter.
     * @param distributedJoinCtx Distributed join context.
     * @param mvccSnapshot MVCC snapshot.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public QueryContext(
        int segment,
        @Nullable IndexingQueryFilter filter,
        @Nullable DistributedJoinContext distributedJoinCtx,
        @Nullable MvccSnapshot mvccSnapshot,
        @Nullable PartitionReservation reservations
    ) {
        this.segment = segment;
        this.filter = filter;
        this.distributedJoinCtx = distributedJoinCtx;
        this.mvccSnapshot = mvccSnapshot;
        this.reservations = reservations;
    }

    /**
     * @return Mvcc snapshot.
     */
    @Nullable public MvccSnapshot mvccSnapshot() {
        return mvccSnapshot;
    }

    /**
     * @return Distributed join context.
     */
    @Nullable public DistributedJoinContext distributedJoinContext() {
        return distributedJoinCtx;
    }

    /**
     * @return Index segment ID.
     */
    public int segment() {
        return segment;
    }

    /**
     * @param nodeStop Node is stopping.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void clearContext(boolean nodeStop) {
        if (distributedJoinCtx != null)
            distributedJoinCtx.cancel();

        if (!nodeStop && reservations != null)
            reservations.release();
    }

    /**
     * @return Filter.
     */
    public IndexingQueryFilter filter() {
        return filter;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryContext.class, this);
    }
}
