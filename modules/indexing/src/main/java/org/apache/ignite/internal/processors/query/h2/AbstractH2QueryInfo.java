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
import org.apache.ignite.internal.processors.query.h2.twostep.MapQueryLazyWorker;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.engine.Session;

/**
 * Base H2 query info with commons for MAP, LOCAL, REDUCE queries.
 */
public abstract class AbstractH2QueryInfo {
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

    /** Enforce join order. */
    private final boolean enforceJoinOrder;

    /** Join batch enabled (distributed join). */
    private final boolean distributedJoin;

    /** Lazy mode. */
    private final boolean lazy;

    /** Begin timestamp. */
    private long timeout;

    /** Begin timestamp. */
    private long timeoutMult;

    /**
     * @param stmt Query statement.
     * @param sql Query statement.
     * @param params Query parameters.
     */
    public AbstractH2QueryInfo(PreparedStatement stmt, String sql, Collection<Object> params) {
        try {
            assert stmt != null;

            this.stmt = stmt;
            this.sql = sql;
            this.params = params;

            beginTs = U.currentTimeMillis();

            schema = stmt.getConnection().getSchema();

            Session s = H2Utils.session(stmt.getConnection());

            enforceJoinOrder = s.isForceJoinOrder();
            distributedJoin = s.isJoinBatchEnabled();
            lazy = s.isLazyQueryExecution() || MapQueryLazyWorker.currentWorker() != null;
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
    protected abstract void printInfo(StringBuilder msg);

    /**
     * @return Query execution time.
     */
    public long time() {
        return U.currentTimeMillis() - beginTs;
    }

    /**
     * @param timeout Query timeout.
     * @param timeoutMult Query timeout multiplier.
     */
    public void setTimeout(long timeout, int timeoutMult) {
        this.timeout = timeout;
        this.timeoutMult = timeoutMult;
    }

    /**
     * @return Query time execution.
     */
    public boolean checkTimeout() {
        if (time() > timeout) {
            if (timeoutMult > 1)
                timeout *= timeoutMult;

            return true;
        }
        else
            return false;
    }

    /**
     * @param log Logger.
     * @param msg Log message
     * @param connMgr Connection manager.
     */
    public void printLogMessage(IgniteLogger log, ConnectionManager connMgr, String msg) {
        Connection c = connMgr.connectionForThread().connection(schema);

        H2Utils.setupConnection(c, distributedJoin, enforceJoinOrder);

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

        StringBuilder msgSb = new StringBuilder(msg + " [");

        printInfo(msgSb);

        msgSb.append(", time=").append(time()).append("ms")
            .append(", distributedJoin=").append(distributedJoin)
            .append(", enforceJoinOrder=").append(enforceJoinOrder)
            .append(", lazy=").append(lazy);

        msgSb.append(", sql='")
            .append(sql)
            .append("', plan=")
            .append(strPlan)
            .append(", parameters=")
            .append(params == null ? "[]" : Arrays.deepToString(params.toArray()))
            .append(']');

        LT.warn(log, msgSb.toString());
    }
}
