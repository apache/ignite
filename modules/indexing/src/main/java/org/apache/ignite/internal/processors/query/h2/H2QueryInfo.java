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
    private final long queryId;

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
            this.nodeId = nodeId;
            this.queryId = queryId;

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
        return U.currentTimeMillis() - beginTs;
    }

    /**
     * @param additionalInfo Additional query info.
     */
    @Override public String queryInfo(@Nullable String additionalInfo) {
        StringBuilder msgSb = new StringBuilder();

        if (queryId == RunningQueryManager.UNDEFINED_QUERY_ID)
            msgSb.append(" [globalQueryId=(undefined), node=").append(nodeId);
        else
            msgSb.append(" [globalQueryId=").append(QueryUtils.globalQueryId(nodeId, queryId));

        if (additionalInfo != null)
            msgSb.append(", ").append(additionalInfo);

        msgSb.append(", duration=").append(time()).append("ms")
                .append(", type=").append(type)
                .append(", distributedJoin=").append(distributedJoin)
                .append(", enforceJoinOrder=").append(enforceJoinOrder)
                .append(", lazy=").append(lazy)
                .append(", schema=").append(schema)
                .append(", sql='").append(sql)
                .append("', plan=").append(stmt.getPlanSQL());

        printInfo(msgSb);

        msgSb.append(']');

        return msgSb.toString();
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
