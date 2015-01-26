/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.query.h2.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.*;
import org.jdk8.backport.*;

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
                assert msg != null;

                ClusterNode node = ctx.discovery().node(nodeId);

                if (msg instanceof GridNextPageResponse)
                    onNextPage(node, (GridNextPageResponse)msg);
                else if (msg instanceof GridQueryFailResponse)
                    onFail(node, (GridQueryFailResponse)msg);

                return true;
            }
        });
    }

    private void onFail(ClusterNode node, GridQueryFailResponse msg) {
        U.error(log, "Failed to execute query.", msg.error());
    }

    private void onNextPage(final ClusterNode node, GridNextPageResponse msg) {
        final long qryReqId = msg.queryRequestId();
        final int qry = msg.query();
        final int pageSize = msg.rows().size();

        QueryRun r = runs.get(qryReqId);

        GridMergeIndex idx = r.tbls.get(msg.query()).getScanIndex(null);

        if (msg.allRows() != -1) { // Only the first page contains row count.
            idx.addCount(msg.allRows());

            r.latch.countDown();
        }

        idx.addPage(new GridResultPage<UUID>(node.id(), msg) {
            @Override public void fetchNextPage() {
                try {
                    ctx.io().sendUserMessage(F.asList(node), new GridNextPageRequest(qryReqId, qry, pageSize));
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
     * @return Future.
     */
    public IgniteFuture<GridCacheSqlResult> query(String space, GridCacheTwoStepQuery qry) {
        long qryReqId = reqIdGen.incrementAndGet();

        QueryRun r = new QueryRun();

        r.tbls = new ArrayList<>(qry.mapQueries().size());

        try {
            r.conn = h2.connectionForSpace(space);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFutureEx<>(e);
        }

        Collection<ClusterNode> nodes = ctx.grid().cluster().nodes(); // TODO filter nodes somehow?

        for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
            GridMergeTable tbl;

            try {
                tbl = createTable(r.conn, mapQry);
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFutureEx<>(e);
            }

            tbl.getScanIndex(null).setNumberOfSources(nodes.size());

            r.tbls.add(tbl);
        }

        r.latch = new CountDownLatch(r.tbls.size() * nodes.size());

        this.runs.put(qryReqId, r);

        try {
            ctx.io().sendUserMessage(nodes, new GridQueryRequest(qryReqId, 1000, qry.mapQueries()), // TODO conf page size
                GridTopic.TOPIC_QUERY, false, 0);

            r.latch.await();

            GridCacheSqlQuery rdc = qry.reduceQuery();

            final ResultSet res = h2.executeSqlQueryWithTimer(r.conn, rdc.query(), F.asList(rdc.parameters()));

            for (GridMergeTable tbl : r.tbls)
                dropTable(r.conn, tbl.getName());

            return new GridFinishedFuture(ctx, new Iter(res));
        }
        catch (IgniteCheckedException | InterruptedException | SQLException e) {
            U.closeQuiet(r.conn);

            return new GridFinishedFuture<>(ctx, e);
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
    }

    /**
     *
     */
    private static class Iter extends GridH2ResultSetIterator<List<?>> implements GridCacheSqlResult {
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
