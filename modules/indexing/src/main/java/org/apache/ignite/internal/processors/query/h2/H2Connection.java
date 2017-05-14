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
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.util.GridCancelable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.schema.Schema;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_STATEMENT_CACHE_SIZE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser.prepared;

/**
 * Pooled H2 connection with statement cache inside.
 */
public final class H2Connection implements GridCancelable {
    /** */
    private static final int PREPARED_STMT_CACHE_SIZE = IgniteSystemProperties.getInteger(
        IGNITE_H2_INDEXING_STATEMENT_CACHE_SIZE, 256);

    /** */
    private static final ConcurrentMap<Session,GridH2QueryContext> sesLocQctx =
        new ConcurrentHashMap<>();

    /** */
    private final Connection conn;

    /** */
    private final Statement stmt;

    /** */
    private final StatementCache stmtCache = new StatementCache(PREPARED_STMT_CACHE_SIZE);

    /** */
    private String schema;

    /** */
    private final H2ConnectionPool pool;

    /** */
    private final Session ses;

    /** */
    private volatile boolean destroyed;

    /**
     * @param dbUrl Database URL.
     */
    public H2Connection(H2ConnectionPool pool, String dbUrl) throws SQLException {
        assert !F.isEmpty(dbUrl): dbUrl;

        this.pool = pool;
        this.conn = DriverManager.getConnection(dbUrl);
        stmt = conn.createStatement();

        // Need to take session here because on connection close we can loose it too early.
        ses = (Session)((JdbcConnection)conn).getSession();
    }

    /**
     * @param ses Session.
     * @return Session local query context.
     */
    public static GridH2QueryContext getQueryContextForSession(Session ses) {
        return ses == null ? null : sesLocQctx.get(ses);
    }

    /**
     * @param qctx Current session query context.
     */
    public void setQueryContextForSession(GridH2QueryContext qctx) {
        assert qctx != null;

        if (sesLocQctx.put(ses, qctx) != null)
            throw new IllegalStateException("Session local query context already set.");
    }

    /**
     * @param sql SQL.
     * @param params Parameters array.
     * @return Prepared statement.
     * @throws SQLException If failed.
     */
    public PreparedStatement prepare(String sql, Object[] params) throws SQLException {
        PreparedStatement ps = stmtCache.get(sql);

        if (ps != null) {
            // We should never close prepared statements, because they are all cacheable.
            if (ps.isClosed()) {
                stmtCache.remove(sql);

                throw new IllegalStateException("Cached prepared statements must never be closed: \n" + sql);
            }

            // If db schema was changed we can return outdated prepared statement here.
            // This is usually ok for execution because it will recompile itself,
            // but Ignite SQL parser will fail.
            if (prepared(ps).needRecompile())
                ps = null;
        }

        if (ps == null) {
            ps = conn.prepareStatement(sql);

            stmtCache.put(sql, ps);
        }

        bindParameters(ps, params);

        return ps;
    }

    /**
     * Binds parameters to prepared statement.
     *
     * @param stmt Prepared statement.
     * @param params Parameters array.
     */
    private void bindParameters(PreparedStatement stmt, @Nullable Object[] params) throws SQLException {
        if (!F.isEmpty(params)) {
            int idx = 1;

            for (Object arg : params)
                bindObject(stmt, idx++, arg);
        }
    }

    /**
     * Binds object to prepared statement.
     *
     * @param stmt SQL statement.
     * @param idx Index.
     * @param obj Value to store.
     */
    private void bindObject(PreparedStatement stmt, int idx, @Nullable Object obj) throws SQLException {
        if (obj == null)
            stmt.setNull(idx, Types.VARCHAR);
        else
            stmt.setObject(idx, obj);
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
    public void schema(String schema) throws SQLException {
        assert schema != null;

        if (F.eq(this.schema, schema))
            return;

        // TODO conn.setSchema(schema);
        // TODO need to check correct case + quotes

        stmt.executeUpdate("SET SCHEMA " + schema);

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
     * Allows to execute simple queries and fetching result set right away.
     *
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
        ses.setForceJoinOrder(enforceJoinOrder);
        ses.setJoinBatchEnabled(distributedJoins);
    }

    /**
     * @param timeout Timeout in milliseconds.
     */
    public void queryTimeout(int timeout) {
        ses.setQueryTimeout(timeout);
    }

    /**
     * @return Current schema object.
     */
    public Schema getCurrentSchema() {
        return ses.getDatabase().getSchema(ses.getCurrentSchemaName());
    }

    /**
     * Destroy the connection.
     */
    public void destroy() {
        if (destroyed)
            return;

        destroyed = true;

        clearSessionLocalQueryContext();

        U.closeQuiet(conn);
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        destroy();
    }

    /**
     */
    public void clearSessionLocalQueryContext() {
        sesLocQctx.remove(ses);
    }

    /**
     * Return to the pool.
     *
     * @throws SQLException If failed.
     */
    public void returnToPool() throws SQLException {
        if (pool == null)
            throw new IllegalStateException("Connection is not pooled.");

        pool.put(this);
    }

    /**
     * @return {@code true} If the connection is still valid.
     * @throws SQLException If failed.
     */
    public boolean isValid() throws SQLException {
        if (destroyed)
            return false;

        synchronized (conn) { // Possible NPE in H2 with racy close.
            return !conn.isClosed();
        }
    }

    /**
     * Drops cached statement.
     */
    public void dropCachedStatement(String sql) {
        stmtCache.remove(sql);
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
