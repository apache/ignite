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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.jdbc.JdbcStatement;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper to store connection with currently used schema and statement cache.
 */
public class H2Connection implements AutoCloseable {
    /** */
    private static final int STATEMENT_CACHE_SIZE = 256;

    /** */
    private final Connection conn;

    /** */
    private volatile String schema;

    /** */
    private volatile H2StatementCache statementCache;

    /** Logger. */
    private IgniteLogger log;

    /**
     * @param conn Connection to use.
     * @param log Logger.
     */
    H2Connection(Connection conn, IgniteLogger log) {
        this.conn = conn;
        this.log = log;

        initStatementCache();
    }

    /**
     * @return Schema name if schema is set, null otherwise.
     */
    String schema() {
        return schema;
    }

    /**
     * @param schema Schema name set on this connection.
     */
    void schema(@Nullable String schema) {
        if (schema != null && !F.eq(this.schema, schema)) {
            try {
                if (schema.trim().isEmpty()) {
                    throw new IgniteSQLException("Failed to set schema for DB connection. " +
                        "Schema name could not be an empty string");
                }

                this.schema = schema;

                conn.setSchema(schema);
            }
            catch (SQLException e) {
                throw new IgniteSQLException("Failed to set schema for DB connection for thread [schema=" +
                    schema + "]", e);
            }
        }
    }

    /**
     * @return Connection.
     */
    Connection connection() {
        return conn;
    }

    /**
     * Clears statement cache.
     */
    void clearStatementCache() {
        initStatementCache();
    }

    /**
     * @return Statement cache.
     */
    H2StatementCache statementCache() {
        return statementCache;
    }

    /**
     * @return Statement cache size.
     */
    public int statementCacheSize() {
        return statementCache == null ? 0 : statementCache.size();
    }

    /**
     * Initializes statement cache.
     */
    private void initStatementCache() {
        statementCache = new H2StatementCache(STATEMENT_CACHE_SIZE);
    }

    /**
     * Prepare statement caching it if needed.
     *
     * @param sql SQL.
     * @return Prepared statement.
     */
    PreparedStatement prepareStatement(String sql, byte qryFlags) {
        try {
            PreparedStatement stmt = cachedPreparedStatement(sql, qryFlags);

            if (stmt == null) {
                H2CachedStatementKey key = new H2CachedStatementKey(schema, sql, qryFlags);

                stmt = prepareStatementNoCache(sql);

                statementCache.put(key, stmt);
            }

            return stmt;
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Failed to parse query. " + e.getMessage(), IgniteQueryErrorCode.PARSING, e);
        }
    }

    /**
     * Get cached prepared statement (if any).
     *
     * @param sql SQL.
     * @return Prepared statement or {@code null}.
     * @throws SQLException On error.
     */
    private @Nullable PreparedStatement cachedPreparedStatement(String sql, byte qryFlags) throws SQLException {
        H2CachedStatementKey key = new H2CachedStatementKey(schema, sql, qryFlags);

        PreparedStatement stmt = statementCache.get(key);

        // Nothing found.
        if (stmt == null)
            return null;

        // Is statement still valid?
        if (
            stmt.isClosed() ||                                 // Closed.
                stmt.unwrap(JdbcStatement.class).isCancelled() ||  // Cancelled.
                GridSqlQueryParser.prepared(stmt).needRecompile() // Outdated (schema has been changed concurrently).
        ) {
            statementCache.remove(schema, sql, qryFlags);

            return null;
        }

        return stmt;
    }

    /**
     * Get prepared statement without caching.
     *
     * @param sql SQL.
     * @return Prepared statement.
     */
    PreparedStatement prepareStatementNoCache(String sql) {
        try {
            return conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Failed to parse query. " + e.getMessage(), IgniteQueryErrorCode.PARSING, e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2Connection.class, this);
    }

    /** Closes wrapped connection (return to pool or close). */
    @Override public void close() {
        U.close(conn, log);
    }
}
