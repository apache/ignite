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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.Prepared;
import org.h2.command.dml.Query;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectUnion;
import org.jetbrains.annotations.NotNull;

/**
 * Long running query manager.
 */
public class IgniteH2QueryInfo implements Comparable<IgniteH2QueryInfo> {
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

    /**
     * Constructor to remove object from collection.
     *
     * @param stmt Query statement.
     */
    public IgniteH2QueryInfo(PreparedStatement stmt) {
        this(stmt, null, null, null);
    }

    /**
     * @param stmt Query statement.
     * @param schema Query schema.
     * @param sql Query statement.
     * @param params Query parameters.
     */
    public IgniteH2QueryInfo(PreparedStatement stmt, String schema, String sql, Collection<Object> params) {
        assert stmt != null;

        this.stmt = stmt;
        this.sql = sql;
        this.params = params;
        this.schema = schema;

        this.beginTs = U.currentTimeMillis();
        qctx = GridH2QueryContext.get();
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
    @Override public int compareTo(@NotNull IgniteH2QueryInfo o) {
        return Long.compare(beginTs, o.beginTs);
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
            log.warning("Cannot get plan for long query: "  + sql, e);
        }

        StringBuilder msg = new StringBuilder("Query execution is too long [time=")
            .append(time()).append("ms")
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
}
