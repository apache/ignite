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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.Objects;
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
    /**
     * Thread local query context is used for API that doesn't support h2 Session:
     * distributed join and rowCount.
     */
    private static final ThreadLocal<QueryContext> qctxThreaded = new ThreadLocal<>();

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

    /** {@code True} for local queries, {@code false} for distributed ones. */
    private final boolean loc;

    /**
     * Constructor.
     *
     * @param segment Index segment ID.
     * @param filter Filter.
     * @param distributedJoinCtx Distributed join context.
     * @param mvccSnapshot MVCC snapshot.
     * @param loc {@code True} for local queries, {@code false} for distributed ones.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public QueryContext(
        int segment,
        @Nullable IndexingQueryFilter filter,
        @Nullable DistributedJoinContext distributedJoinCtx,
        @Nullable MvccSnapshot mvccSnapshot,
        @Nullable PartitionReservation reservations,
        boolean loc
    ) {
        this.segment = segment;
        this.filter = filter;
        this.distributedJoinCtx = distributedJoinCtx;
        this.mvccSnapshot = mvccSnapshot;
        this.reservations = reservations;
        this.loc = loc;
    }

    /**
     * @param filter Filter.
     * @param local Local query flag.
     * @return Context for parsing.
     */
    public static QueryContext parseContext(@Nullable IndexingQueryFilter filter, boolean local) {
        return new QueryContext(
            0,
            filter,
            null,
            null,
            null,
            local);
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

    /**
     * @return {@code True} for local queries, {@code false} for distributed ones.
     */
    public boolean local() {
        return loc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryContext.class, this);
    }

    /**
     * Hack with thread local context is used only for H2 methods that is called without Session object.
     *  e.g. GridH2Table.getRowCountApproximation (used only on optimization phase, after parse).
     *
     * @param qctx Context.
     */
    public static void threadLocal(QueryContext qctx) {
        qctxThreaded.set(qctx);
    }

    /**
     * Hack with thread local context is used only for H2 methods that is called without Session object.
     *  e.g. GridH2Table.getRowCountApproximation (used only on optimization phase, after parse).
     *
     * @return Thread local context.
     */
    public static QueryContext threadLocal() {
        QueryContext qctx = qctxThreaded.get();

        assert Objects.nonNull(qctx);

        return qctx;
    }
}
