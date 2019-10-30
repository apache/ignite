/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Pooled connection wrapper to use close semantic to recycle connection (return to the pool).
 */
public class H2PooledConnection implements AutoCloseable {
    /** */
    private volatile H2Connection delegate;

    /** Connection manager. */
    private final ConnectionManager connMgr;

    /** Closed (recycled) flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * @param conn Connection to use.
     * @param connMgr Connection manager is use to recycle connection
     *      (connection is closed or returned to connection pool).
     */
    H2PooledConnection(H2Connection conn, ConnectionManager connMgr) {
        this.delegate = conn;
        this.connMgr = connMgr;
    }

    /**
     * @return Schema name if schema is set, null otherwise.
     */
    public String schema() {
        return delegate.schema();
    }

    /**
     * @param schema Schema name set on this connection.
     */
    public void schema(@Nullable String schema) {
        delegate.schema(schema);
    }

    /**
     * @return Connection.
     */
    public Connection connection() {
        return delegate.connection();
    }

    /**
     * @return Statement cache size.
     */
    public int statementCacheSize() {
        return delegate.statementCacheSize();
    }

    /**
     * Prepare statement caching it if needed.
     *
     * @param sql SQL.
     * @return Prepared statement.
     * @throws SQLException If failed.
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return delegate.prepareStatement(sql);
    }

    /**
     * Get prepared statement without caching.
     *
     * @param sql SQL.
     * @return Prepared statement.
     * @throws SQLException If failed.
     */
    public PreparedStatement prepareStatementNoCache(String sql) throws SQLException {
        return delegate.prepareStatementNoCache(sql);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2PooledConnection.class, this);
    }

    /** Closes wrapped connection (return to pool or close). */
    @Override public void close() {
        assert delegate != null;

        if (closed.compareAndSet(false, true)) {
            H2Utils.resetSession(this);

            connMgr.recycle(delegate);

            delegate = null;
        }
    }
}
