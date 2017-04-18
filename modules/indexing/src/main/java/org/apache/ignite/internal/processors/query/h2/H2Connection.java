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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_STATEMENT_CACHE_SIZE;

/**
 * Pooled H2 connection with statement cache inside.
 */
public final class H2Connection implements AutoCloseable {
    /** The period of clean up the connection from pool. */
    private static final long CLEANUP_PERIOD = IgniteSystemProperties.getLong(
        IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD, 30_000);

    /** */
    private static final int PREPARED_STMT_CACHE_SIZE = IgniteSystemProperties.getInteger(
        IGNITE_H2_INDEXING_STATEMENT_CACHE_SIZE, 256);

    /** */
    private static final AtomicIntegerFieldUpdater<H2Connection> deadUpd =
        AtomicIntegerFieldUpdater.newUpdater(H2Connection.class, "dead");

    /** */
    @SuppressWarnings("unused")
    private volatile int dead;

    /** */
    private final Connection conn;

    /** */
    private final Statement stmt;

    /** */
    private final long createTime = U.currentTimeMillis();

    /** */
    private final StatementCache stmtCache = new StatementCache(PREPARED_STMT_CACHE_SIZE);

    /** */
    private String schema;

    /** */
    private final H2ConnectionPool pool;

    /**
     * @param dbUrl Database URL.
     */
    public H2Connection(H2ConnectionPool pool, String dbUrl) throws SQLException {
        assert !F.isEmpty(dbUrl): dbUrl;

        this.conn = DriverManager.getConnection(dbUrl);
        stmt = conn.createStatement();
        this.pool = pool;
    }

    /**
     * @param qctx Current session query context.
     */
    public void setQueryContext(GridH2QueryContext qctx) {
        H2ConnectionPool.queryContext(session(), qctx);
    }

    /**
     * @param sql SQL.
     * @param cache If we need to cache the statement for reuse.
     * @return Prepared statement.
     * @throws SQLException If failed.
     */
    public PreparedStatement prepare(String sql, boolean cache) throws SQLException {
        PreparedStatement ps = cache ? stmtCache.get(sql) : null;

        if (ps == null)
            ps = conn.prepareStatement(sql);

        return ps;
    }

    /**
     * @return Schema name if schema is set, null otherwise.
     */
    public String schema() {
        return schema;
    }

    /**
     * @param schema Schema name set on this connection.
     */
    public void schema(String schema) throws IgniteCheckedException {
        assert schema != null;

        if (F.eq(this.schema, schema))
            return;

        // TODO conn.setSchema(schema);

        try {
            stmt.executeUpdate("SET SCHEMA " + schema);
        }
        catch (SQLException e) {
            throw new IgniteCheckedException(e);
        }

        this.schema = schema;
    }

    /**
     * @param sql SQL Command.
     * @throws SQLException If failed.
     */
    public void executeUpdate(String sql) throws SQLException {
        stmt.executeUpdate(sql);
    }

    /**
     * @param sql SQL query.
     * @return Result set.
     * @throws SQLException If failed.
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        return stmt.executeQuery(sql);
    }

    /**
     * @param distributedJoins If distributed joins are enabled.
     * @param enforceJoinOrder Enforce join order of tables.
     */
    public void setupConnection(boolean distributedJoins, boolean enforceJoinOrder) {
        Session s = session();

        s.setForceJoinOrder(enforceJoinOrder);
        s.setJoinBatchEnabled(distributedJoins);
    }

    public void queryTimeout(int timeout) {
        session().setQueryTimeout(timeout);
    }

    /**
     * @throws SQLException If failed.
     */
    public void destroy() throws SQLException {
        if (!deadUpd.compareAndSet(this, 0, 1))
            return;

        Session ses = session();

        if (ses != null)
            H2ConnectionPool.removeQueryContext(ses);

        if (!conn.isClosed())
            conn.close();
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        if (pool == null)
            destroy();
        else
            pool.put(this);
    }

    /**
     * @return {@code true} If the connection is still valid.
     * @throws SQLException If failed.
     */
    public boolean isValid() throws SQLException {
        return dead == 0 && !conn.isClosed() &&
            (U.currentTimeMillis() - createTime) < CLEANUP_PERIOD;
    }

    /**
     * @return Session.
     */
    public Session session() {
        return (Session)((JdbcConnection)conn).getSession();
    }

    /**
     * Statement cache.
     */
    private static final class StatementCache extends LinkedHashMap<String, PreparedStatement> {
        /** */
        private int maxSize;

        /**
         * @param maxSize Size.
         */
        private StatementCache(int maxSize) {
            super(maxSize, (float)0.75, true);

            this.maxSize = maxSize;
        }

        /** {@inheritDoc} */
        @Override protected boolean removeEldestEntry(Map.Entry<String, PreparedStatement> eldest) {
            boolean rmv = size() > maxSize;

            if (rmv) {
                PreparedStatement stmt = eldest.getValue();

                U.closeQuiet(stmt);
            }

            return rmv;
        }
    }
}
