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
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.jetbrains.annotations.Nullable;

/**
 * Mapper query results.
 */
class MapQueryResults {
    /** H2 indexing. */
    private final IgniteH2Indexing h2;

    private final IgniteLogger log;

    /** */
    private final long qryReqId;

    /** */
    private final AtomicReferenceArray<MapQueryResult> results;

    /** */
    private final GridQueryCancel[] cancels;

    /** */
    private final GridCacheContext<?, ?> cctx;

    /** Lazy worker. */
    private final MapQueryLazyWorker lazyWorker;

    /** */
    private volatile boolean cancelled;

    /**
     * Constructor.
     *
     * @param qryReqId Query request ID.
     * @param qrys Number of queries.
     * @param cctx Cache context.
     * @param lazyWorker Lazy worker (if any).
     */
    @SuppressWarnings("unchecked")
    MapQueryResults(IgniteH2Indexing h2, long qryReqId, int qrys, @Nullable GridCacheContext<?, ?> cctx,
        @Nullable MapQueryLazyWorker lazyWorker) {
        this.h2 = h2;
        this.qryReqId = qryReqId;
        this.cctx = cctx;
        this.lazyWorker = lazyWorker;

        results = new AtomicReferenceArray<>(qrys);
        cancels = new GridQueryCancel[qrys];

        log = cctx.logger(MapQueryResults.class);

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
     * @return Lazy worker.
     */
    MapQueryLazyWorker lazyWorker() {
        return lazyWorker;
    }

    /**
     * Add result.
     *
     * @param qry Query result index.
     * @param q Query object.
     * @param qrySrcNodeId Query source node.
     * @param rs Result set.
     */
    void addResult(int qry, GridCacheSqlQuery q, UUID qrySrcNodeId, ResultSet rs, Object[] params) {
        MapQueryResult res = new MapQueryResult(h2, rs, cctx, qrySrcNodeId, q, params);

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
    void cancel() {
        if (cancelled)
            return;

        cancelled = true;

        for (int i = 0; i < results.length(); i++) {
            GridQueryCancel cancel = cancels[i];

            if (cancel != null)
                cancel.cancel();
        }
    }

    /**
     * Close the query.
     */
    void close() {
        if (lazyWorker == null || !lazyWorker.isStarted() || MapQueryLazyWorker.currentWorker() != null) {
            for (int i = 0; i < results.length(); i++) {
                MapQueryResult res = results.get(i);

                if (res != null) {
                    res.close();

                    continue;
                }
            }

            if (lazyWorker != null)
                lazyWorker.stop(false);
        }
        else {
            lazyWorker.submit(new Runnable() {
                @Override public void run() {
                    close();
                }
            });

            lazyWorker.awaitStop();
        }
    }

    /**
     * @return Cancel flag.
     */
    boolean cancelled() {
        return cancelled;
    }

    /**
     * @return Query request ID.
     */
    long queryRequestId() {
        return qryReqId;
    }
}
