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

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.jdbc.JdbcResultSet;
import org.h2.result.LazyResult;
import org.h2.result.ResultInterface;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;

/**
 * Mapper result for a single part of the query.
 */
class MapQueryResult {
    /** */
    private static final Field RESULT_FIELD;

    /*
     * Initialize.
     */
    static {
        try {
            RESULT_FIELD = JdbcResultSet.class.getDeclaredField("result");

            RESULT_FIELD.setAccessible(true);
        }
        catch (NoSuchFieldException e) {
            throw new IllegalStateException("Check H2 version in classpath.", e);
        }
    }

    /** Indexing. */
    private final IgniteH2Indexing h2;

    /** */
    private final ResultInterface res;

    /** */
    private final ResultSet rs;

    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final GridCacheSqlQuery qry;

    /** */
    private final UUID qrySrcNodeId;

    /** */
    private final int cols;

    /** */
    private int page;

    /** */
    private final int rowCnt;

    /** */
    private boolean cpNeeded;

    /** */
    private volatile boolean closed;

    /** */
    private final Object[] params;

    /** Lazy worker. */
    private final MapQueryLazyWorker lazyWorker;

    /**
     * @param rs Result set.
     * @param cctx Cache context.
     * @param qrySrcNodeId Query source node.
     * @param qry Query.
     * @param params Query params.
     * @param lazyWorker Lazy worker.
     */
    MapQueryResult(IgniteH2Indexing h2, ResultSet rs, @Nullable GridCacheContext cctx,
        UUID qrySrcNodeId, GridCacheSqlQuery qry, Object[] params, @Nullable MapQueryLazyWorker lazyWorker) {
        this.h2 = h2;
        this.cctx = cctx;
        this.qry = qry;
        this.params = params;
        this.qrySrcNodeId = qrySrcNodeId;
        this.cpNeeded = F.eq(h2.kernalContext().localNodeId(), qrySrcNodeId);
        this.lazyWorker = lazyWorker;

        if (rs != null) {
            this.rs = rs;

            try {
                res = (ResultInterface)RESULT_FIELD.get(rs);
            }
            catch (IllegalAccessException e) {
                throw new IllegalStateException(e); // Must not happen.
            }

            rowCnt = (res instanceof LazyResult) ? -1 : res.getRowCount();
            cols = res.getVisibleColumnCount();
        }
        else {
            this.rs = null;
            this.res = null;
            this.cols = -1;
            this.rowCnt = -1;

            closed = true;
        }
    }

    /**
     * @return Page number.
     */
    int page() {
        return page;
    }

    /**
     * @return Row count.
     */
    int rowCount() {
        return rowCnt;
    }

    /**
     * @return Column ocunt.
     */
    int columnCount() {
        return cols;
    }

    /**
     * @return Closed flag.
     */
    boolean closed() {
        return closed;
    }

    /**
     * @param rows Collection to fetch into.
     * @param pageSize Page size.
     * @return {@code true} If there are no more rows available.
     */
    synchronized boolean fetchNextPage(List<Value[]> rows, int pageSize) {
        assert lazyWorker == null || lazyWorker == MapQueryLazyWorker.currentWorker();

        if (closed)
            return true;

        boolean readEvt = cctx != null && cctx.name() != null && cctx.events().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

        page++;

        for (int i = 0 ; i < pageSize; i++) {
            if (!res.next())
                return true;

            Value[] row = res.currentRow();

            if (cpNeeded) {
                boolean copied = false;

                for (int j = 0; j < row.length; j++) {
                    Value val = row[j];

                    if (val instanceof GridH2ValueCacheObject) {
                        GridH2ValueCacheObject valCacheObj = (GridH2ValueCacheObject)val;

                        row[j] = new GridH2ValueCacheObject(valCacheObj.getCacheObject(), h2.objectContext()) {
                            @Override public Object getObject() {
                                return getObject(true);
                            }
                        };

                        copied = true;
                    }
                }

                if (i == 0 && !copied)
                    cpNeeded = false; // No copy on read caches, skip next checks.
            }

            assert row != null;

            if (readEvt) {
                GridKernalContext ctx = h2.kernalContext();

                ctx.event().record(new CacheQueryReadEvent<>(
                    ctx.discovery().localNode(),
                    "SQL fields query result set row read.",
                    EVT_CACHE_QUERY_OBJECT_READ,
                    CacheQueryType.SQL.name(),
                    cctx.name(),
                    null,
                    qry.query(),
                    null,
                    null,
                    params,
                    qrySrcNodeId,
                    null,
                    null,
                    null,
                    null,
                    row(row)));
            }

            rows.add(res.currentRow());
        }

        return !res.hasNext();
    }

    /**
     * @param row Values array row.
     * @return Objects list row.
     */
    private List<?> row(Value[] row) {
        List<Object> res = new ArrayList<>(row.length);

        for (Value v : row)
            res.add(v.getObject());

        return res;
    }

    /**
     * Close the result.
     */
    public void close() {
        if (lazyWorker != null && MapQueryLazyWorker.currentWorker() == null) {
            lazyWorker.submit(new Runnable() {
                @Override public void run() {
                    close();
                }
            });

            lazyWorker.awaitStop();

            return;
        }

        synchronized (this) {
            assert lazyWorker == null || lazyWorker == MapQueryLazyWorker.currentWorker();

            if (closed)
                return;

            closed = true;

            U.closeQuiet(rs);

            if (lazyWorker != null)
                lazyWorker.stop(false);
        }
    }
}
