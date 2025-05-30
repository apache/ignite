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
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.running.RunningQueryManager;
import org.apache.ignite.internal.processors.query.running.TrackableQueryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.jetbrains.annotations.Nullable;

/**
 * Base H2 query info with commons for MAP, LOCAL, REDUCE queries.
 */
public class H2QueryInfo extends TrackableQueryImpl {
    /** Type. */
    private QueryType type;

    /** Begin timestamp. */
    private long beginTs;

    /** The most recent point in time when the tracking of a long query was suspended. */
    private volatile long lastSuspendTs;

    /** External wait time. */
    private volatile long extWait;

    /** Long query time tracking suspension flag. */
    private volatile boolean isSuspended;

    /** Query SQL. */
    private final String sql;

    /** Enforce join order. */
    private final boolean enforceJoinOrder;

    /** Join batch enabled (distributed join). */
    private final boolean distributedJoin;

    /** Lazy mode. */
    private final boolean lazy;

    /** Prepared statement. */
    private Prepared stmt;

    /** Query SQL plan. */
    private volatile String plan;

    /**
     * @param type Query type.
     * @param stmt Query statement.
     * @param sql Query statement.
     * @param nodeId Originator node id.
     * @param queryId Query id.
     */
    public H2QueryInfo(QueryType type, PreparedStatement stmt, String sql, UUID nodeId, long queryId) {
        try {
            assert stmt != null;

            this.type = type;
            this.sql = sql;

            nodeId(nodeId);
            queryId(queryId);

            beginTs = U.currentTimeMillis();

            schema(stmt.getConnection().getSchema());

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
    public synchronized String plan() {
        if (plan == null) {
            String plan0 = stmt.getPlanSQL();

            plan = (plan0 != null) ? planWithoutScanCount(plan0) : "";
        }

        return plan;
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

        if (queryId() == RunningQueryManager.UNDEFINED_QUERY_ID)
            msgSb.append(" [globalQueryId=(undefined), node=").append(nodeId());
        else
            msgSb.append(" [globalQueryId=").append(QueryUtils.globalQueryId(nodeId(), queryId()));

        if (additionalInfo != null)
            msgSb.append(", ").append(additionalInfo);

        msgSb.append(", duration=").append(time()).append("ms")
                .append(", type=").append(type)
                .append(", distributedJoin=").append(distributedJoin)
                .append(", enforceJoinOrder=").append(enforceJoinOrder)
                .append(", lazy=").append(lazy)
                .append(", schema=").append(schema())
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
        String res = null;

        int start = plan.indexOf("\n    /* scanCount");

        if (start != -1) {
            int end = plan.indexOf("*/", start);

            res = plan.substring(0, start) + plan.substring(end + 2);
        }

        return (res == null) ? plan : res;
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
