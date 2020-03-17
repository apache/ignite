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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.twostep.MapQueryLazyWorker;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.Prepared;
import org.h2.command.dml.Query;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectUnion;
import org.h2.engine.Session;

/**
 * Base H2 query info with commons for MAP, LOCAL, REDUCE queries.
 */
public class H2QueryInfo {
    /** Type. */
    private final QueryType type;

    /** Begin timestamp. */
    private final long beginTs;

    /** Query schema. */
    private final String schema;

    /** Query SQL. */
    private final String sql;

    /** Enforce join order. */
    private final boolean enforceJoinOrder;

    /** Join batch enabled (distributed join). */
    private final boolean distributedJoin;

    /** Lazy mode. */
    private final boolean lazy;

    /** Prepared statement. */
    private final PreparedStatement stmt;

    /**
     * @param type Query type.
     * @param stmt Query statement.
     * @param sql Query statement.
     */
    public H2QueryInfo(QueryType type, PreparedStatement stmt, String sql) {
        try {
            assert stmt != null;

            this.type = type;
            this.sql = sql;

            beginTs = U.currentTimeMillis();

            schema = stmt.getConnection().getSchema();

            Session s = H2Utils.session(stmt.getConnection());

            enforceJoinOrder = s.isForceJoinOrder();
            distributedJoin = s.isJoinBatchEnabled();
            lazy = s.isLazyQueryExecution() || MapQueryLazyWorker.currentWorker() != null;
            this.stmt = stmt;
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Cannot collect query info", IgniteQueryErrorCode.UNKNOWN, e);
        }
    }

    /**
     * Print info specified by children.
     *
     * @param msg Message string builder.
     */
    protected void printInfo(StringBuilder msg) {
        // No-op.
    };

    /**
     * @return Query execution time.
     */
    public long time() {
        return U.currentTimeMillis() - beginTs;
    }

    /**
     * @param log Logger.
     * @param msg Log message
     * @param additionalInfo Additional query info.
     */
    public void printLogMessage(IgniteLogger log, String msg, String additionalInfo) {
        printLogMessage(log, null, msg, additionalInfo);
    }

    /**
     * @param log Logger.
     * @param msg Log message
     * @param connMgr Connection manager.
     * @param additionalInfo Additional query info.
     */
    public void printLogMessage(IgniteLogger log, ConnectionManager connMgr, String msg, String additionalInfo) {
        Prepared prep = GridSqlQueryParser.prepared(stmt);

        assert prep instanceof Query;

        StringBuilder msgSb = new StringBuilder(msg + " [");

        if (additionalInfo != null)
            msgSb.append(additionalInfo).append(", ");

        msgSb.append("duration=").append(time()).append("ms")
            .append(", type=").append(type)
            .append(", distributedJoin=").append(distributedJoin)
            .append(", enforceJoinOrder=").append(enforceJoinOrder)
            .append(", lazy=").append(lazy)
            .append(", scansCount=[").append(scanCounts((Query)prep)).append(']');

        msgSb.append(", sql='")
            .append(sql);

        if (type != QueryType.REDUCE && connMgr != null)
            msgSb.append("', plan=").append(queryPlan(log, connMgr));

        printInfo(msgSb);

        msgSb.append(']');

        LT.warn(log, msgSb.toString());
    }

    /**
     * @param log Logger.
     * @param connMgr Connection manager.
     * @return Query plan.
     */
    protected String queryPlan(IgniteLogger log, ConnectionManager connMgr) {
        if (!S.includeSensitive())
            return "<sensitive info is hidden>";

        return GridSqlQueryParser.prepared(stmt).getPlanSQL();
    }


    /**
     * Prints scanCount for all TableFilters of a query.
     * @param qry H2 query.
     * @return String representation of all scans count.
     */
    private static String scanCounts(Query qry) {
        StringBuilder scanCnt = new StringBuilder();

        printScanCounts(qry, scanCnt);

        return scanCnt.toString();
    }

    /**
     * @param qry H2 query.
     * @param sb Output string builder.
     */
    private static void printScanCounts(Query qry, final StringBuilder sb) {
        if (qry.isUnion()) {
            SelectUnion union = (SelectUnion)qry;

            printScanCounts(union.getLeft(), sb);
            printScanCounts(union.getRight(), sb);
        }
        else {
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
    }

    /**
     * @return Lazy flag.
     */
    public boolean lazy() {
        return lazy;
    }

    /**
     * Query type.
     */
    public enum QueryType {
        LOCAL,
        MAP,
        REDUCE
    }
}
