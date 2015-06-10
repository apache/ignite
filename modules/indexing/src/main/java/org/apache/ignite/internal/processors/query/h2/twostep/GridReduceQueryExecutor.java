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
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.query.h2.*;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.h2.command.*;
import org.h2.command.ddl.*;
import org.h2.command.dml.Query;
import org.h2.engine.*;
import org.h2.expression.*;
import org.h2.index.*;
import org.h2.jdbc.*;
import org.h2.result.*;
import org.h2.table.*;
import org.h2.tools.*;
import org.h2.util.*;
import org.h2.value.*;
import org.jsr166.*;

import javax.cache.*;
import java.lang.reflect.*;
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

    /** */
    private static ThreadLocal<GridMergeTable> curFunTbl = new ThreadLocal<>();

    /** */
    private static final Constructor<JdbcResultSet> CONSTRUCTOR;

    /**
     * Init constructor.
     */
    static {
        try {
            CONSTRUCTOR = JdbcResultSet.class.getDeclaredConstructor(
                JdbcConnection.class,
                JdbcStatement.class,
                ResultInterface.class,
                Integer.TYPE,
                Boolean.TYPE,
                Boolean.TYPE,
                Boolean.TYPE
            );

            CONSTRUCTOR.setAccessible(true);
        }
        catch (NoSuchMethodException e) {
            throw new IllegalStateException("Check H2 version in classpath.", e);
        }
    }

    /** */
    private final GridSpinBusyLock busyLock;

    /**
     * @param busyLock Busy lock.
     */
    public GridReduceQueryExecutor(GridSpinBusyLock busyLock) {
        this.busyLock = busyLock;
    }

    /**
     * @param ctx Context.
     * @param h2 H2 Indexing.
     * @throws IgniteCheckedException If failed.
     */
    public void start(final GridKernalContext ctx, IgniteH2Indexing h2) throws IgniteCheckedException {
        this.ctx = ctx;
        this.h2 = h2;

        log = ctx.log(GridReduceQueryExecutor.class);

        ctx.io().addMessageListener(GridTopic.TOPIC_QUERY, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                if (!busyLock.enterBusy())
                    return;

                try {
                    GridReduceQueryExecutor.this.onMessage(nodeId, msg);
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
        });

        ctx.event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(final Event evt) {
                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                for (QueryRun r : runs.values()) {
                    for (GridMergeTable tbl : r.tbls) {
                        if (tbl.getScanIndex(null).hasSource(nodeId)) {
                            fail(r, nodeId, "Node left the topology.");

                            break;
                        }
                    }
                }
            }
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    public void onMessage(UUID nodeId, Object msg) {
        try {
            assert msg != null;

            ClusterNode node = ctx.discovery().node(nodeId);

            if (node == null)
                return; // Node left, ignore.

            boolean processed = true;

            if (msg instanceof GridQueryNextPageResponse)
                onNextPage(node, (GridQueryNextPageResponse)msg);
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
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void onFail(ClusterNode node, GridQueryFailResponse msg) {
        QueryRun r = runs.get(msg.queryRequestId());

        fail(r, node.id(), msg.error());
    }

    /**
     * @param r Query run.
     * @param nodeId Failed node ID.
     * @param msg Error message.
     */
    private void fail(QueryRun r, UUID nodeId, String msg) {
        if (r != null) {
            r.rmtErr = new CacheException("Failed to execute map query on the node: " + nodeId + ", " + msg);

            while(r.latch.getCount() != 0)
                r.latch.countDown();

            for (GridMergeTable tbl : r.tbls)
                tbl.getScanIndex(null).fail(nodeId);
        }
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void onNextPage(final ClusterNode node, GridQueryNextPageResponse msg) {
        final long qryReqId = msg.queryRequestId();
        final int qry = msg.query();

        final QueryRun r = runs.get(qryReqId);

        if (r == null) // Already finished with error or canceled.
            return;

        final int pageSize = r.pageSize;

        GridMergeIndex idx = r.tbls.get(msg.query()).getScanIndex(null);

        GridResultPage page;

        try {
            page = new GridResultPage(ctx, node.id(), msg, false) {
                @Override public void fetchNextPage() {
                    if (r.rmtErr != null)
                        throw new CacheException("Next page fetch failed.", r.rmtErr);

                    try {
                        GridQueryNextPageRequest msg0 = new GridQueryNextPageRequest(qryReqId, qry, pageSize);

                        if (node.isLocal())
                            h2.mapQueryExecutor().onMessage(ctx.localNodeId(), msg0);
                        else
                            ctx.io().send(node, GridTopic.TOPIC_QUERY, msg0, GridIoPolicy.PUBLIC_POOL);
                    }
                    catch (IgniteCheckedException e) {
                        throw new CacheException(e);
                    }
                }
            };
        }
        catch (Exception e) {
            U.error(log, "Error in message.", e);

            fail(r, node.id(), "Error in message.");

            return;
        }

        idx.addPage(page);

        if (msg.allRows() != -1) // Only the first page contains row count.
            r.latch.countDown();
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @return Cursor.
     */
    public Iterator<List<?>> query(GridCacheContext<?,?> cctx, GridCacheTwoStepQuery qry, boolean keepPortable) {
        long qryReqId = reqIdGen.incrementAndGet();

        QueryRun r = new QueryRun();

        r.pageSize = qry.pageSize() <= 0 ? GridCacheTwoStepQuery.DFLT_PAGE_SIZE : qry.pageSize();

        r.tbls = new ArrayList<>(qry.mapQueries().size());

        String space = cctx.name();

        r.conn = (JdbcConnection)h2.connectionForSpace(space);

        // TODO    Add topology version.
        ClusterGroup dataNodes = ctx.grid().cluster().forDataNodes(space);

        if (cctx.isReplicated() || qry.explain()) {
            assert qry.explain() || dataNodes.node(ctx.localNodeId()) == null : "We must be on a client node.";

            // Select random data node to run query on a replicated data or get EXPLAIN PLAN from a single node.
            dataNodes = dataNodes.forRandom();
        }

        final Collection<ClusterNode> nodes = dataNodes.nodes();

        for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
            GridMergeTable tbl;

            try {
                tbl = createFunctionTable(r.conn, mapQry, qry.explain()); // createTable(r.conn, mapQry); TODO
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            GridMergeIndex idx = tbl.getScanIndex(null);

            for (ClusterNode node : nodes)
                idx.addSource(node.id());

            r.tbls.add(tbl);

            curFunTbl.set(tbl);
        }

        r.latch = new CountDownLatch(r.tbls.size() * nodes.size());

        runs.put(qryReqId, r);

        try {
            Collection<GridCacheSqlQuery> mapQrys = qry.mapQueries();

            if (qry.explain()) {
                mapQrys = new ArrayList<>(qry.mapQueries().size());

                for (GridCacheSqlQuery mapQry : qry.mapQueries())
                    mapQrys.add(new GridCacheSqlQuery(mapQry.alias(), "EXPLAIN " + mapQry.query(), mapQry.parameters()));
            }

            if (nodes.size() != 1 || !F.first(nodes).isLocal()) { // Marshall params for remotes.
                Marshaller m = ctx.config().getMarshaller();

                for (GridCacheSqlQuery mapQry : mapQrys)
                    mapQry.marshallParams(m);
            }

            send(nodes, new GridQueryRequest(qryReqId, r.pageSize, space, mapQrys));

            r.latch.await();

            if (r.rmtErr != null)
                throw new CacheException("Failed to run map query remotely.", r.rmtErr);

            if (qry.explain())
                return explainPlan(r.conn, space, qry);

            GridCacheSqlQuery rdc = qry.reduceQuery();

            final ResultSet res = h2.executeSqlQueryWithTimer(space, r.conn, rdc.query(), F.asList(rdc.parameters()));

            for (GridMergeTable tbl : r.tbls) {
                if (!tbl.getScanIndex(null).fetchedAll()) // We have to explicitly cancel queries on remote nodes.
                    send(nodes, new GridQueryCancelRequest(qryReqId));

//                dropTable(r.conn, tbl.getName()); TODO
            }

            return new GridQueryCacheObjectsIterator(new Iter(res), cctx, keepPortable);
        }
        catch (IgniteCheckedException | InterruptedException | RuntimeException e) {
            U.closeQuiet(r.conn);

            if (e instanceof CacheException)
                throw (CacheException)e;

            throw new CacheException("Failed to run reduce query locally.", e);
        }
        finally {
            if (!runs.remove(qryReqId, r))
                U.warn(log, "Query run was already removed: " + qryReqId);

            curFunTbl.remove();
        }
    }

    /**
     * @param c Connection.
     * @param space Space.
     * @param qry Query.
     * @return Cursor for plans.
     * @throws IgniteCheckedException if failed.
     */
    private Iterator<List<?>> explainPlan(JdbcConnection c, String space, GridCacheTwoStepQuery qry)
        throws IgniteCheckedException {
        List<List<?>> lists = new ArrayList<>();

        for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
            ResultSet rs = h2.executeSqlQueryWithTimer(space, c, "SELECT PLAN FROM " + mapQry.alias(), null);

            lists.add(F.asList(getPlan(rs)));
        }

        for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
            GridMergeTable tbl = createFunctionTable(c, mapQry, false);

            curFunTbl.set(tbl); // Now it will be only a single table.
        }

        GridCacheSqlQuery rdc = qry.reduceQuery();

        ResultSet rs = h2.executeSqlQueryWithTimer(space, c, "EXPLAIN " + rdc.query(), F.asList(rdc.parameters()));

        lists.add(F.asList(getPlan(rs)));

        return lists.iterator();
    }

    /**
     * @param rs Result set.
     * @return Plan.
     * @throws IgniteCheckedException If failed.
     */
    private String getPlan(ResultSet rs) throws IgniteCheckedException {
        try {
            if (!rs.next())
                throw new IllegalStateException();

            return rs.getString(1);
        }
        catch (SQLException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param nodes Nodes.
     * @param msg Message.
     * @throws IgniteCheckedException If failed.
     */
    private void send(Collection<ClusterNode> nodes, Message msg) throws IgniteCheckedException {
        for (ClusterNode node : nodes) {
            if (node.isLocal()) {
                if (nodes.size() > 1) {
                    ArrayList<ClusterNode> remotes = new ArrayList<>(nodes.size() - 1);

                    for (ClusterNode node0 : nodes) {
                        if (!node0.isLocal())
                            remotes.add(node0);
                    }

                    assert remotes.size() == nodes.size() - 1;

                    ctx.io().send(remotes, GridTopic.TOPIC_QUERY, msg, GridIoPolicy.PUBLIC_POOL);
                }

                // Local node goes the last to allow parallel execution.
                h2.mapQueryExecutor().onMessage(ctx.localNodeId(), msg);

                return;
            }
        }

        // All the given nodes are remotes.
        ctx.io().send(nodes, GridTopic.TOPIC_QUERY, msg, GridIoPolicy.PUBLIC_POOL);
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
     * @return Merged result set.
     */
    public static ResultSet mergeTableFunction(JdbcConnection c) throws Exception {
        GridMergeTable tbl = curFunTbl.get();

        Session ses = (Session)c.getSession();

        String url = c.getMetaData().getURL();

        // URL is either "jdbc:default:connection" or "jdbc:columnlist:connection"
        final Cursor cursor = url.charAt(5) == 'c' ? null : tbl.getScanIndex(ses).find(ses, null, null);

        final Column[] cols = tbl.getColumns();

        SimpleResultSet rs = new SimpleResultSet(cursor == null ? null : new SimpleRowSource() {
            @Override public Object[] readRow() throws SQLException {
                if (!cursor.next())
                    return null;

                Row r = cursor.get();

                Object[] row = new Object[cols.length];

                for (int i = 0; i < row.length; i++)
                    row[i] = r.getValue(i).getObject();

                return row;
            }

            @Override public void close() {
                // No-op.
            }

            @Override public void reset() throws SQLException {
                throw new SQLException("Unsupported.");
            }
        }) {
            @Override public byte[] getBytes(int colIdx) throws SQLException {
                assert cursor != null;

                return cursor.get().getValue(colIdx - 1).getBytes();
            }

            @Override public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
                throw new UnsupportedOperationException();
            }

            @Override public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
                throw new UnsupportedOperationException();
            }
        };

        for (Column col : cols)
            rs.addColumn(col.getName(), DataType.convertTypeToSQLType(col.getType()),
                MathUtils.convertLongToInt(col.getPrecision()), col.getScale());

        return rs;
    }

    /**
     * @param asQuery Query.
     * @return List of columns.
     */
    private static ArrayList<Column> generateColumnsFromQuery(org.h2.command.dml.Query asQuery) {
        int columnCount = asQuery.getColumnCount();
        ArrayList<Expression> expressions = asQuery.getExpressions();
        ArrayList<Column> cols = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            int type = expr.getType();
            String name = expr.getAlias();
            long precision = expr.getPrecision();
            int displaySize = expr.getDisplaySize();
            DataType dt = DataType.getDataType(type);
            if (precision > 0 && (dt.defaultPrecision == 0 ||
                (dt.defaultPrecision > precision && dt.defaultPrecision < Byte.MAX_VALUE))) {
                // dont' set precision to MAX_VALUE if this is the default
                precision = dt.defaultPrecision;
            }
            int scale = expr.getScale();
            if (scale > 0 && (dt.defaultScale == 0 ||
                (dt.defaultScale > scale && dt.defaultScale < precision))) {
                scale = dt.defaultScale;
            }
            if (scale > precision) {
                precision = scale;
            }
            Column col = new Column(name, type, precision, scale, displaySize);
            cols.add(col);
        }

        return cols;
    }

    /**
     * @param conn Connection.
     * @param qry Query.
     * @param explain Explain.
     * @return Table.
     * @throws IgniteCheckedException
     */
    private GridMergeTable createFunctionTable(JdbcConnection conn, GridCacheSqlQuery qry, boolean explain)
        throws IgniteCheckedException {
        try {
            Session ses = (Session)conn.getSession();

            CreateTableData data  = new CreateTableData();

            data.tableName = "T___";
            data.schema = ses.getDatabase().getSchema(ses.getCurrentSchemaName());
            data.create = true;

            if (!explain) {
                Prepared prepare = ses.prepare(qry.query(), false);

                List<org.h2.expression.Parameter> parsedParams = prepare.getParameters();

                for (int i = Math.min(parsedParams.size(), qry.parameters().length); --i >= 0; ) {
                    Object val = qry.parameters()[i];

                    parsedParams.get(i).setValue(DataType.convertToValue(ses, val, Value.UNKNOWN));
                }

                data.columns = generateColumnsFromQuery((Query)prepare);
            }
            else
                data.columns = planColumns();

            return new GridMergeTable(data);
        }
        catch (Exception e) {
            U.closeQuiet(conn);

            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @return Columns.
     */
    private static ArrayList<Column> planColumns() {
        ArrayList<Column> res = new ArrayList<>(1);

        res.add(new Column("PLAN", Value.STRING));

        return res;
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
        private JdbcConnection conn;

        /** */
        private int pageSize;

        /** */
        private volatile CacheException rmtErr;
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
