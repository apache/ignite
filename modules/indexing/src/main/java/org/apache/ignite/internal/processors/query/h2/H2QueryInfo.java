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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.running.RunningQueryManager;
import org.apache.ignite.internal.processors.query.running.TrackableQuery;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.jetbrains.annotations.Nullable;

/**
 * Base H2 query info with commons for MAP, LOCAL, REDUCE queries.
 */
public class H2QueryInfo implements TrackableQuery {
    /** Type. */
    private final QueryType type;

    /** Begin timestamp. */
    private final long beginTs;

    /** The most recent point in time when the tracking of a long query was suspended. */
    private volatile long lastSuspendTs;

    /** External wait time. */
    private volatile long extWait;

    /** Long query time tracking suspension flag. */
    private volatile boolean isSuspended;

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
    private final Prepared stmt;

    /** Originator node uid. */
    private final UUID nodeId;

    /** Query id. */
    private final long qryId;

    /** Query initiator ID. */
    private final String initiatorId;

    /** Query SQL plan. */
    private volatile String plan;

    /**
     * @param type Query type.
     * @param stmt Query statement.
     * @param sql Query statement.
     * @param nodeId Originator node id.
     * @param qryId Query id.
     * @param initiatorId Query initiator id.
     */
    public H2QueryInfo(QueryType type, PreparedStatement stmt, String sql, UUID nodeId, long qryId, String initiatorId) {
        try {
            assert stmt != null;

            this.type = type;
            this.sql = sql;
            this.nodeId = nodeId;
            this.qryId = qryId;
            this.initiatorId = initiatorId;

            beginTs = U.currentTimeMillis();

            schema = stmt.getConnection().getSchema();

            Session s = H2Utils.session(stmt.getConnection());

            enforceJoinOrder = s.isForceJoinOrder();
            distributedJoin = s.isJoinBatchEnabled();
            lazy = s.isLazyQueryExecution();
            this.stmt = GridSqlQueryParser.prepared(stmt);
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Cannot collect query info", IgniteQueryErrorCode.UNKNOWN, e);
        }
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public long queryId() {
        return qryId;
    }

    /** */
    public synchronized String plan() {
        if (plan == null) {
            String plan0 = stmt.getPlanSQL();

            plan = (plan0 != null) ? cleanPlan(plan0) : "";
        }

        return plan;
    }

    /**
     * @param plan Plan.
     */
    private String cleanPlan(String plan) {
        plan = planWithoutScanCount(plan);
        plan = planWithoutSystemAliases(plan);

        return plan;
    }

    /** */
    public String schema() {
        return schema;
    }

    /** */
    public String sql() {
        return sql;
    }

    /** */
    public long extWait() {
        return extWait;
    }

    /**
     * Print info specified by children.
     *
     * @param msg Message string builder.
     */
    protected void printInfo(StringBuilder msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long time() {
        return (isSuspended ? lastSuspendTs : U.currentTimeMillis()) - beginTs - extWait;
    }

    /** */
    public synchronized void suspendTracking() {
        if (!isSuspended) {
            isSuspended = true;

            lastSuspendTs = U.currentTimeMillis();
        }
    }

    /** */
    public synchronized void resumeTracking() {
        if (isSuspended) {
            isSuspended = false;

            extWait += U.currentTimeMillis() - lastSuspendTs;
        }
    }

    /**
     * @param additionalInfo Additional query info.
     */
    @Override public String queryInfo(@Nullable String additionalInfo) {
        StringBuilder msgSb = new StringBuilder();

        if (qryId == RunningQueryManager.UNDEFINED_QUERY_ID)
            msgSb.append(" [globalQueryId=(undefined), node=").append(nodeId);
        else
            msgSb.append(" [globalQueryId=").append(QueryUtils.globalQueryId(nodeId, qryId));

        if (additionalInfo != null)
            msgSb.append(", ").append(additionalInfo);

        msgSb.append(", duration=").append(time()).append("ms")
                .append(", type=").append(type)
                .append(", distributedJoin=").append(distributedJoin)
                .append(", enforceJoinOrder=").append(enforceJoinOrder)
                .append(", lazy=").append(lazy)
                .append(", schema=").append(schema)
                .append(", initiatorId=").append(initiatorId)
                .append(", sql='").append(sql)
                .append("', plan=").append(plan());

        printInfo(msgSb);

        msgSb.append(']');

        return msgSb.toString();
    }

    /** */
    public boolean isSuspended() {
        return isSuspended;
    }

    /**
     * If the same SQL query is executed sequentially within a single instance of {@link H2Connection} (which happens,
     * for example, during consecutive local query executions), the next execution plan is generated using
     * a {@link PreparedStatement} stored in the statement cache of this connection — H2Connection#statementCache.<br>
     * <br>
     * During the preparation of a PreparedStatement, a TableFilter object is created, where the variable scanCount
     * stores the number of elements scanned within the query. If this value is not zero, the generated execution plan
     * will contain a substring in the following format: "scanCount: X", where X is the value of the scanCount
     * variable at the time of plan generation.<br>
     * <br>
     * The scanCount variable is reset in the TableFilter#startQuery method. However, since execution plans are
     * generated and recorded asynchronously, there is no guarantee that plan generation happens immediately after
     * TableFilter#startQuery is called.<br>
     * <br>
     * As a result, identical execution plans differing only by the scanCount suffix may be recorded in the SQL plan
     * history. To prevent this, the suffix should be removed from the plan as soon as it is generated with the
     * Prepared#getPlanSQL method.<br>
     * <br>
     *
     * @param plan SQL plan.
     *
     * @return SQL plan without the scanCount suffix.
     */
    public String planWithoutScanCount(String plan) {
        if (!plan.contains("scanCount:"))
            return plan;

        StringBuilder res = new StringBuilder(plan);

        removeLineWithPattern(res, "/* scanCount:", "*/");
        removeLineWithPattern(res, "/++ scanCount:", "++/");

        return res.toString();
    }

    /**
     * @param sb StringBuilder.
     * @param startPattern Start pattern.
     * @param endMarker End marker.
     */
    private void removeLineWithPattern(StringBuilder sb, String startPattern, String endMarker) {
        int start = sb.indexOf(startPattern, 0);

        while (start != -1) {
            while (start > 0 && sb.charAt(start) != '\n')
                --start;

            int end = sb.indexOf(endMarker, start);

            if (end == -1)
                break;

            sb.delete(start, end + endMarker.length());

            start = sb.indexOf(startPattern, start);
        }
    }

    /**
     * Normalizes H2 auto-generated numeric aliases (e.g. "_1", "_4") in a plan to make plan history stable
     * across repeated executions of the same logical query.
     */
    private String planWithoutSystemAliases(String plan) {
        if (plan.indexOf('_') < 0)
            return plan;

        int n = plan.length();

        Map<String, String> aliasMap = new HashMap<>();
        StringBuilder out = new StringBuilder(n);

        for (int l = 0; l < n; ) {
            char c = plan.charAt(l);

            if (c != '_') {
                out.append(c);
                ++l;
                continue;
            }

            if (l > 0 && plan.charAt(l - 1) != ' ') {
                out.append(c);
                ++l;
                continue;
            }

            int r = l + 1;

            if (r >= n || !Character.isDigit(plan.charAt(r))) {
                out.append(c);
                ++l;
                continue;
            }

            while (r < n && Character.isDigit(plan.charAt(r)))
                ++r;

            if (r < n) {
                char next = plan.charAt(r);

                if (next != '.' && next != ' ' && next != '\n') {
                    out.append(c);
                    ++l;
                    continue;
                }
            }

            String token = plan.substring(l, r);

            String repl = aliasMap.computeIfAbsent(token, k -> "__A" + aliasMap.size());

            out.append(repl);

            l = r;
        }

        return out.toString();
    }

    /**
     * Query type.
     */
    public enum QueryType {
        /** */
        LOCAL,

        /** */
        MAP,

        /** */
        REDUCE
    }
}
