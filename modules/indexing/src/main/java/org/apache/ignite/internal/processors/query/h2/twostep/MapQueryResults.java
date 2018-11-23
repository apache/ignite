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

import java.sql.ResultSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.h2.H2ConnectionWrapper;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.ObjectPoolReusable;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.h2.engine.Session;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode.OFF;

/**
 * Mapper query results.
 */
class MapQueryResults {
    /** H2 indexing. */
    private final IgniteH2Indexing h2;

    /** */
    private final long qryReqId;

    /** */
    private final AtomicReferenceArray<MapQueryResult> results;

    /** */
    private final GridQueryCancel[] cancels;

    /** */
    private final GridCacheContext<?, ?> cctx;

    /** Lazy mode. */
    private boolean lazy;

    /** */
    private volatile boolean cancelled;

    /** {@code SELECT FOR UPDATE} flag. */
    private final boolean forUpdate;

    /** Query context. */
    private final GridH2QueryContext qctx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param h2 Indexing instance.
     * @param qryReqId Query request ID.
     * @param qrys Number of queries.
     * @param cctx Cache context.
     * @param forUpdate {@code SELECT FOR UPDATE} flag.
     * @param lazy Lazy flag.
     * @param qctx Query context.
     * @param log Logger instance.
     */
    MapQueryResults(IgniteH2Indexing h2, long qryReqId, int qrys, @Nullable GridCacheContext<?, ?> cctx,
        boolean forUpdate, boolean lazy, GridH2QueryContext qctx, IgniteLogger log) {
        this.forUpdate = forUpdate;
        this.h2 = h2;
        this.qryReqId = qryReqId;
        this.cctx = cctx;
        this.lazy = lazy;
        this.qctx = qctx;
        this.log = log;

        results = new AtomicReferenceArray<>(qrys);
        cancels = new GridQueryCancel[qrys];

        for (int i = 0; i < cancels.length; i++)
            cancels[i] = new GridQueryCancel();
    }

    /**
     * @param qry Query result index.
     * @return Query result.
     */
    MapQueryResult result(int qry) {
        return results.get(qry);
    }

    /**
     * Get cancel token for query.
     *
     * @param qryIdx Query index.
     * @return Cancel token.
     */
    GridQueryCancel queryCancel(int qryIdx) {
        return cancels[qryIdx];
    }

    /**
     * Add result.
     * @param qry Query result index.
     * @param q Query object.
     * @param qrySrcNodeId Query source node.
     * @param rs Result set.
     * @param params Query arguments.
     * @param session H2 Session.
     */
    void addResult(int qry, GridCacheSqlQuery q, UUID qrySrcNodeId, ResultSet rs, Object[] params,
        Session session) {
        MapQueryResult res = new MapQueryResult(h2, rs, cctx, qrySrcNodeId, q, params, session, log);

        if (!results.compareAndSet(qry, null, res))
            throw new IllegalStateException();
    }

    /**
     * @return {@code true} If all results are closed.
     */
    boolean isAllClosed() {
        for (int i = 0; i < results.length(); i++) {
            MapQueryResult res = results.get(i);

            if (res == null || !res.closed())
                return false;
        }

        return true;
    }

    /**
     * Cancels the query.
     */
    synchronized void cancel() {
        if (cancelled)
            return;

        cancelled = true;

        for (int i = 0; i < results.length(); i++) {
            GridQueryCancel cancel = cancels[i];

            if (cancel != null)
                cancel.cancel();
        }

        close();
    }

    /**
     *
     */
    public synchronized void close() {
        for (int i = 0; i < results.length(); i++) {
            MapQueryResult res = results.get(i);

            if (res != null)
                res.close();
        }

        if (lazy)
            release();
    }

    /**
     * @return Cancel flag.
     */
    boolean cancelled() {
        return cancelled;
    }

    /**
     * @return Query lazy mode.
     */
    public boolean lazy() {
        return lazy;
    }

    /**
     * @return Query request ID.
     */
    long queryRequestId() {
        return qryReqId;
    }

    /**
     * @return {@code SELECT FOR UPDATE} flag.
     */
    public boolean isForUpdate() {
        return forUpdate;
    }

    /**
     * @return Query context.
     */
    public GridH2QueryContext queryContext() {
        return qctx;
    }

    /**
     */
    public void release() {
        GridH2QueryContext.clearThreadLocal();

        if (qctx.distributedJoinMode() == OFF)
            qctx.clearContext(false);
    }
}
