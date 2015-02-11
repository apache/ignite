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

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.query.h2.*;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jdk8.backport.*;

import javax.cache.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Reduce query executor.
 */
public class GridReduceQueryExecutor {
    /** */
    private GridKernalContext ctx;

    /** */
    private IgniteH2Indexing h2;

    /** */
    private IgniteLogger log;

    /** */
    private final AtomicLong reqIdGen = new AtomicLong();

    /** */
    private final ConcurrentMap<Long, QueryRun> runs = new ConcurrentHashMap8<>();

    /**
     * @param ctx Context.
     * @param h2 H2 Indexing.
     * @throws IgniteCheckedException If failed.
     */
    public void start(final GridKernalContext ctx, IgniteH2Indexing h2) throws IgniteCheckedException {
        this.ctx = ctx;
        this.h2 = h2;

        log = ctx.log(GridReduceQueryExecutor.class);

        // TODO handle node failure.

        ctx.io().addUserMessageListener(GridTopic.TOPIC_QUERY, new IgniteBiPredicate<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                try {
                    assert msg != null;

                    ClusterNode node = ctx.discovery().node(nodeId);

                    boolean processed = true;

                    if (msg instanceof GridNextPageResponse)
                        onNextPage(node, (GridNextPageResponse)msg);
                    else if (msg instanceof GridQueryFailResponse)
                        onFail(node, (GridQueryFailResponse)msg);
                    else
                        processed = false;

                    if (processed && log.isDebugEnabled())
                        log.debug("Processed response: " + nodeId + "->" + ctx.localNodeId() + " " + msg);
                }
                catch(Throwable th) {
                    U.error(log, "Failed to process message: " + msg, th);
                }

                return true;
            }
        });
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void onFail(ClusterNode node, GridQueryFailResponse msg) {
        QueryRun r = runs.get(msg.queryRequestId());

        if (r != null && r.latch.getCount() != 0) {
            r.rmtErr = msg.error();

            while(r.latch.getCount() > 0)
                r.latch.countDown();
        }
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void onNextPage(final ClusterNode node, GridNextPageResponse msg) {
        final long qryReqId = msg.queryRequestId();
        final int qry = msg.query();
        final int pageSize = msg.rows().size();

        QueryRun r = runs.get(qryReqId);

        if (r == null) // Already finished with error or canceled.
            return;

        GridMergeIndex idx = r.tbls.get(msg.query()).getScanIndex(null);

        if (msg.allRows() != -1) { // Only the first page contains row count.
            idx.addCount(msg.allRows());

            r.latch.countDown();
        }

        idx.addPage(new GridResultPage<UUID>(node.id(), msg) {
            @Override public void fetchNextPage() {
                if (res.isLast())
                    return; // No-op if this message known to be the last.

                try {
                    ctx.io().sendUserMessage(F.asList(node), new GridNextPageRequest(qryReqId, qry, pageSize),
                        GridTopic.TOPIC_QUERY, false, 0);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        });
    }

    /**
     * @param space Space name.
     * @param qry Query.
     * @return Cursor.
     */
    public QueryCursor<List<?>> query(String space, GridCacheTwoStepQuery qry) {
        long qryReqId = reqIdGen.incrementAndGet();

        QueryRun r = new QueryRun();

        r.tbls = new ArrayList<>(qry.mapQueries().size());

        r.conn = h2.connectionForSpace(space);

        // TODO Add topology version.
        Collection<ClusterNode> nodes = ctx.grid().cluster().forCacheNodes(space).nodes();

        for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
            GridMergeTable tbl;

            try {
                tbl = createTable(r.conn, mapQry);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            tbl.getScanIndex(null).setNumberOfSources(nodes.size());

            r.tbls.add(tbl);
        }

        r.latch = new CountDownLatch(r.tbls.size() * nodes.size());

        runs.put(qryReqId, r);

        try {
            ctx.io().sendUserMessage(nodes, new GridQueryRequest(qryReqId, 1000, qry.mapQueries()), // TODO conf page size
                GridTopic.TOPIC_QUERY, false, 0);

            r.latch.await();

            if (r.rmtErr != null)
                throw new CacheException("Failed to run map query remotely.", r.rmtErr);

            GridCacheSqlQuery rdc = qry.reduceQuery();

            final ResultSet res = h2.executeSqlQueryWithTimer(r.conn, rdc.query(), F.asList(rdc.parameters()));

            for (GridMergeTable tbl : r.tbls)
                dropTable(r.conn, tbl.getName());

            return new QueryCursorImpl<>(new Iter(res));
        }
        catch (IgniteCheckedException | InterruptedException | SQLException | RuntimeException e) {
            U.closeQuiet(r.conn);

            if (e instanceof CacheException)
                throw (CacheException)e;

            throw new CacheException("Failed to run reduce query locally.", e);
        }
        finally {
            if (!runs.remove(qryReqId, r))
                U.warn(log, "Query run was removed: " + qryReqId);
        }
    }

    /**
     * @param conn Connection.
     * @param tblName Table name.
     * @throws SQLException If failed.
     */
    private void dropTable(Connection conn, String tblName) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE " + tblName);
        }
    }

    /**
     * @param conn Connection.
     * @param qry Query.
     * @return Table.
     * @throws IgniteCheckedException If failed.
     */
    private GridMergeTable createTable(Connection conn, GridCacheSqlQuery qry) throws IgniteCheckedException {
        try {
            try (PreparedStatement s = conn.prepareStatement(
                "CREATE LOCAL TEMPORARY TABLE " + qry.alias() +
                " ENGINE \"" + GridMergeTable.Engine.class.getName() + "\" " +
                " AS SELECT * FROM (" + qry.query() + ") WHERE FALSE")) {
                h2.bindParameters(s, F.asList(qry.parameters()));

                s.execute();
            }

            return GridMergeTable.Engine.getCreated();
        }
        catch (SQLException e) {
            U.closeQuiet(conn);

            throw new IgniteCheckedException(e);
        }
    }

    /**
     *
     */
    private static class QueryRun {
        /** */
        private List<GridMergeTable> tbls;

        /** */
        private CountDownLatch latch;

        /** */
        private Connection conn;

        /** */
        private volatile Throwable rmtErr;
    }

    /**
     *
     */
    private static class Iter extends GridH2ResultSetIterator<List<?>> {
        /** */
        private static final long serialVersionUID = 0L;
        
        /**
         * @param data Data array.
         * @throws IgniteCheckedException If failed.
         */
        protected Iter(ResultSet data) throws IgniteCheckedException {
            super(data);
        }

        /** {@inheritDoc} */
        @Override protected List<?> createRow() {
            ArrayList<Object> res = new ArrayList<>(row.length);

            Collections.addAll(res, row);

            return res;
        }
    }
}
