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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.Prepared;
import org.h2.command.dml.Query;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectUnion;
import org.h2.engine.Session;

/**
 * Long running query manager.
 */
public class IgniteH2QueryInfo {
    /** Begin timestamp. */
    private final long beginTs;

    /** Query statement. */
    private final PreparedStatement stmt;

    /** Query schema. */
    private final String schema;

    /** Query SQL. */
    private final String sql;

    /** Query params. */
    private final Collection<Object> params;

    /** Query context. */
    private final GridH2QueryContext qctx;

    /** Enforce join order. */
    private final boolean enforceJoinOrder;

    /** Join batch enabled (distributed join). */
    private final boolean distributedJoin;

    /** Lazy mode. */
    private final boolean lazy;

    /**
     * Constructor to remove object from collection.
     *
     * @param stmt Query statement.
     */
    public IgniteH2QueryInfo(PreparedStatement stmt) {
        this(stmt, null, null, null, false, false, false);
    }

    /**
     * @param stmt Query statement.
     * @param schema Query schema.
     * @param sql Query statement.
     * @param params Query parameters.
     * @param distributedJoin Distributed join.
     * @param enforceJoinOrder Enforce join order flag.
     * @param lazy Lazy mode flag.
     */
    private IgniteH2QueryInfo(PreparedStatement stmt, String schema, String sql, Collection<Object> params,
        boolean distributedJoin, boolean enforceJoinOrder, boolean lazy) {
        assert stmt != null;

        this.stmt = stmt;
        this.sql = sql;
        this.params = params;
        this.schema = schema;

        beginTs = U.currentTimeMillis();
        qctx = GridH2QueryContext.get();

        this.enforceJoinOrder = enforceJoinOrder;
        this.distributedJoin = distributedJoin;
        this.lazy = lazy;
    }

    /**
     * @param qry H2 query.
     * @param sb Output string builder.
     */
    public static void printScanCounts(Query qry, final StringBuilder sb) {
        if (qry instanceof Select) {
            Select select = (Select)qry;

            select.getTopTableFilter().visit(f -> {
                if (sb.length() > 0)
                    sb.append(", ");

                sb.append("[alias=").append(f.getTableAlias());

                if (f.getTable() != null)
                    sb.append(", table=").append(f.getTable().getName());

                sb.append(", scan=").append(GridSqlQueryParser.TABLE_FILTER_SCAN_COUNT.get(f));
                sb.append("]");
            });
        }
        else if (qry instanceof SelectUnion) {
            SelectUnion union = (SelectUnion)qry;

            printScanCounts(union.getLeft(), sb);
            printScanCounts(union.getRight(), sb);
        }
    }

    /**
     * @param stmt Query statement.
     * @param sql Query statement.
     * @param params Query parameters.
     * @return Info object.
     */
    public static IgniteH2QueryInfo collectInfo(PreparedStatement stmt, String sql, Collection<Object> params) {
        try {
            String schema = stmt.getConnection().getSchema();

            Session s = H2Utils.session(stmt.getConnection());

            return new IgniteH2QueryInfo(stmt, schema, sql, params,
                s.isJoinBatchEnabled(),
                s.isForceJoinOrder(),
                s.isLazyQueryExecution()
            );
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Cannot collect query info", IgniteQueryErrorCode.UNKNOWN, e);
        }
    }

    /**
     * @param timeout Query timeout.
     * @return {@code true} in case query execution time is too long.
     */
    public boolean isLong(long timeout) {
        return U.currentTimeMillis() - beginTs > timeout;
    }

    /**
     * @return Query time execution.
     */
    public long time() {
        return U.currentTimeMillis() - beginTs;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        IgniteH2QueryInfo info = (IgniteH2QueryInfo)o;

        return F.eq(stmt, info.stmt);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return stmt.hashCode();
    }

    /**
     * @param log Logger.
     * @param connMgr Connection manager.
     */
    public void printLogMessage(IgniteLogger log, ConnectionManager connMgr) {
        Connection c = connMgr.connectionForThread().connection(schema);

        H2Utils.setupConnection(c, distributedJoin, enforceJoinOrder, lazy);

        Prepared prep = GridSqlQueryParser.prepared(stmt);

        assert prep instanceof Query;

        StringBuilder scanCnt = new StringBuilder();

        printScanCounts((Query)prep, scanCnt);

        String strPlan = null;

        try (PreparedStatement pstmt = c.prepareStatement("EXPLAIN " + sql)) {
            H2Utils.bindParameters(pstmt, params);

            try (ResultSet plan = pstmt.executeQuery()) {
                plan.next();

                strPlan = plan.getString(1) + U.nl();
            }
        }
        catch (Exception e) {
            log.warning("Cannot get plan for long query: " + sql, e);
        }

        StringBuilder msg = new StringBuilder("Query execution is too long [time=")
            .append(time()).append("ms")
            .append(", distributedJoin=").append(distributedJoin)
            .append(", enforceJoinOrder=").append(enforceJoinOrder)
            .append(", lazy=").append(lazy)
            .append(", scanCounts=[").append(scanCnt.toString()).append(']');

        if (qctx != null)
            msg.append(", context=" + qctx);

        msg.append(", sql='")
            .append(sql)
            .append("', plan=")
            .append(strPlan)
            .append(", parameters=")
            .append(params == null ? "[]" : Arrays.deepToString(params.toArray()))
            .append(']');

        LT.warn(log, msg.toString());
    }
}
