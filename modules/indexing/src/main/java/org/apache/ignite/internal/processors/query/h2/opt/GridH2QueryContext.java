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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.query.h2.opt.join.DistributedJoinContext;
import org.apache.ignite.internal.processors.query.h2.twostep.MapQueryLazyWorker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.MAP;

/**
 * Thread local SQL query context which is intended to be accessible from everywhere.
 */
public class GridH2QueryContext {
    /** */
    private final QueryContextKey key;

    /** */
    private final IndexingQueryFilter filter;

    /** Distributed join context. */
    private final DistributedJoinContext distributedJoinCtx;

    /** */
    private final MvccSnapshot mvccSnapshot;

    /** */
    private final List<GridReservable> reservations;

    /** */
    private MapQueryLazyWorker lazyWorker;

    /**
     * Constructor.
     *
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param segmentId Index segment ID.
     * @param type Query type.
     * @param filter Filter.
     * @param distributedJoinCtx Distributed join context.
     * @param mvccSnapshot MVCC snapshot.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public GridH2QueryContext(
        UUID nodeId,
        long qryId,
        int segmentId,
        GridH2QueryType type,
        @Nullable IndexingQueryFilter filter,
        @Nullable DistributedJoinContext distributedJoinCtx,
        @Nullable MvccSnapshot mvccSnapshot,
        @Nullable List<GridReservable> reservations
    ) {
        assert segmentId == 0 || type == MAP;

        key = new QueryContextKey(nodeId, qryId, segmentId, type);

        this.filter = filter;
        this.distributedJoinCtx = distributedJoinCtx;
        this.mvccSnapshot = mvccSnapshot;
        this.reservations = reservations;
    }

    /**
     * @return Key.
     */
    public QueryContextKey key() {
        return key;
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
     * @return index segment ID.
     */
    public int segment() {
        return key.segmentId();
    }

    /**
     * @param nodeStop Node is stopping.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void clearContext(boolean nodeStop) {
        if (distributedJoinCtx != null)
            distributedJoinCtx.cancel();

        List<GridReservable> r = reservations;

        if (!nodeStop && !F.isEmpty(r)) {
            for (int i = 0; i < r.size(); i++)
                r.get(i).release();
        }
    }

    /**
     * @return Filter.
     */
    public IndexingQueryFilter filter() {
        return filter;
    }

    /**
     * @return Lazy worker, if any, or {@code null} if none.
     */
    public MapQueryLazyWorker lazyWorker() {
        return lazyWorker;
    }

    /**
     * @param lazyWorker Lazy worker, if any, or {@code null} if none.
     * @return {@code this}.
     */
    public GridH2QueryContext lazyWorker(MapQueryLazyWorker lazyWorker) {
        this.lazyWorker = lazyWorker;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2QueryContext.class, this);
    }
}
