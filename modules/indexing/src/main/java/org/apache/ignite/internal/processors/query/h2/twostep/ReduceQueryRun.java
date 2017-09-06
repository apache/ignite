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

import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.h2.jdbc.JdbcConnection;
import org.jetbrains.annotations.Nullable;

import javax.cache.CacheException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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

    /** Can be either CacheException in case of error or AffinityTopologyVersion to retry if needed. */
    private final AtomicReference<Object> state = new AtomicReference<>();

    /**
     * Constructor.
     *
     * @param id Query ID.
     * @param qry Query text.
     * @param schemaName Schema name.
     * @param conn Connection.
     * @param idxsCnt Number of indexes.
     * @param pageSize Page size.
     * @param startTime Start time.
     * @param cancel Query cancel handler.
     */
    ReduceQueryRun(Long id, String qry, String schemaName, Connection conn, int idxsCnt, int pageSize, long startTime,
        GridQueryCancel cancel) {
        this.qry = new GridRunningQueryInfo(id, qry, SQL_FIELDS, schemaName, startTime, cancel, false);

        this.conn = (JdbcConnection)conn;

        this.idxs = new ArrayList<>(idxsCnt);

        this.pageSize = pageSize > 0 ? pageSize : GridCacheTwoStepQuery.DFLT_PAGE_SIZE;
    }

    /**
     * @param o Fail state object.
     * @param nodeId Node ID.
     */
    void state(Object o, @Nullable UUID nodeId) {
        assert o != null;
        assert o instanceof CacheException || o instanceof AffinityTopologyVersion : o.getClass();

        if (!state.compareAndSet(null, o))
            return;

        while (latch.getCount() != 0) // We don't need to wait for all nodes to reply.
            latch.countDown();

        CacheException e = o instanceof CacheException ? (CacheException) o : null;

        for (GridMergeIndex idx : idxs) // Fail all merge indexes.
            idx.fail(nodeId, e);
    }

    /**
     * @param e Error.
     */
    void disconnected(CacheException e) {
        state(e, null);
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

    /**
     * @return State.
     */
    Object state() {
        return state.get();
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
}
