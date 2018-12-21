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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxSelectForUpdateFuture;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.jdbc.JdbcConnection;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;

/**
 * Query run.
 */
class ReduceQueryRun {
    /** */
    private final GridRunningQueryInfo qry;

    /** */
    private final List<GridMergeIndex> idxs;

    /** */
    private CountDownLatch latch;

    /** */
    private final JdbcConnection conn;

    /** */
    private final int pageSize;

    /** */
    private final AtomicReference<State> state = new AtomicReference<>();

    /** Future controlling {@code SELECT FOR UPDATE} query execution. */
    private final GridNearTxSelectForUpdateFuture selectForUpdateFut;

    /**
     * Constructor.
     * @param id Query ID.
     * @param qry Query text.
     * @param schemaName Schema name.
     * @param conn Connection.
     * @param idxsCnt Number of indexes.
     * @param pageSize Page size.
     * @param startTime Start time.
     * @param selectForUpdateFut Future controlling {@code SELECT FOR UPDATE} query execution.
     * @param cancel Query cancel handler.
     */
    ReduceQueryRun(Long id, String qry, String schemaName, Connection conn, int idxsCnt, int pageSize, long startTime,
        GridNearTxSelectForUpdateFuture selectForUpdateFut, GridQueryCancel cancel) {
        this.qry = new GridRunningQueryInfo(id, qry, SQL_FIELDS, schemaName, startTime, cancel,
            false);

        this.conn = (JdbcConnection)conn;

        this.idxs = new ArrayList<>(idxsCnt);

        this.pageSize = pageSize > 0 ? pageSize : GridCacheTwoStepQuery.DFLT_PAGE_SIZE;

        this.selectForUpdateFut = selectForUpdateFut;
    }

    /**
     * Set state on exception.
     *
     * @param err error.
     * @param nodeId Node ID.
     */
    void setStateOnException(@Nullable UUID nodeId, CacheException err) {
        setState0(new State(nodeId, err, null, null));
    }

    /**
     * Set state on map node leave.
     *
     * @param nodeId Node ID.
     * @param topVer Topology version.
     */
    void setStateOnNodeLeave(UUID nodeId, AffinityTopologyVersion topVer) {
        setState0(new State(nodeId, null, topVer, "Data node has left the grid during query execution [nodeId=" +
            nodeId + ']'));
    }

    /**
     * Set state on retry due to mapping failure.
     *
     * @param nodeId Node ID.
     * @param topVer Topology version.
     * @param retryCause Retry cause.
     */
    void setStateOnRetry(UUID nodeId, AffinityTopologyVersion topVer, String retryCause) {
        assert !F.isEmpty(retryCause);

        setState0(new State(nodeId, null, topVer, retryCause));
    }

    /**
     *
     * @param state state
     */
    private void setState0(State state){
        if (!this.state.compareAndSet(null, state))
            return;

        while (latch.getCount() != 0) // We don't need to wait for all nodes to reply.
            latch.countDown();

        for (GridMergeIndex idx : idxs) // Fail all merge indexes.
            idx.fail(state.nodeId, state.ex);
    }

    /**
     * @param e Error.
     */
    void disconnected(CacheException e) {
        setStateOnException(null, e);
    }

    /**
     * @return Query info.
     */
    GridRunningQueryInfo queryInfo() {
        return qry;
    }

    /**
     * @return Page size.
     */
    int pageSize() {
        return pageSize;
    }

    /**
     * @return Connection.
     */
    JdbcConnection connection() {
        return conn;
    }

    /** */
    boolean hasErrorOrRetry(){
        return state.get() != null;
    }

    /**
     * @return Exception.
     */
    CacheException exception() {
        State st = state.get();

        return st != null ? st.ex : null;
    }

    /**
     * @return Retry topology version.
     */
    AffinityTopologyVersion retryTopologyVersion(){
        State st = state.get();

        return st != null ? st.retryTopVer : null;
    }

    /**
     * @return Retry bode ID.
     */
    UUID retryNodeId() {
        State st = state.get();

        return st != null ? st.nodeId : null;
    }

    /**
     * @return Retry cause.
     */
    String retryCause(){
        State st = state.get();

        return st != null ? st.retryCause : null;
    }

    /**
     * @return Indexes.
     */
    List<GridMergeIndex> indexes() {
        return idxs;
    }

    /**
     * @return Latch.
     */
    CountDownLatch latch() {
        return latch;
    }

    /**
     * @param latch Latch.
     */
    void latch(CountDownLatch latch) {
        this.latch = latch;
    }

    /**
     * @return {@code SELECT FOR UPDATE} future, if any.
     */
    @Nullable public GridNearTxSelectForUpdateFuture selectForUpdateFuture() {
        return selectForUpdateFut;
    }

    /**
     * Error state.
     */
    private static class State {
        /** Affected node (may be null in case of local node failure). */
        private final UUID nodeId;

        /** Error. */
        private final CacheException ex;

        /** Retry topology version. */
        private final AffinityTopologyVersion retryTopVer;

        /** Retry cause. */
        private final String retryCause;

        /** */
        private State(UUID nodeId, CacheException ex, AffinityTopologyVersion retryTopVer, String retryCause){
            this.nodeId = nodeId;
            this.ex = ex;
            this.retryTopVer = retryTopVer;
            this.retryCause = retryCause;
        }
    }
}
