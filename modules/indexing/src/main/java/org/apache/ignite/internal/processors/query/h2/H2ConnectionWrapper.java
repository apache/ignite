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

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Wrapper to store connection, schema set on that connection and
 * a statement cache for that connection.
 */
public class H2ConnectionWrapper implements Closeable {
    /** */
    private static final int PREPARED_STMT_CACHE_SIZE = 256;

    /** */
    private final Connection conn;

    /** */
    private final String schema;

    /** Last usage. */
    private volatile long lastUsage;

    /** Statement cache. */
    private final H2StatementCache stmtCache = new H2StatementCache(PREPARED_STMT_CACHE_SIZE);

    /**
     * @param conn Connection to use.
     * @param schema Schema name.
     */
    H2ConnectionWrapper(Connection conn, String schema) {
        this.conn = conn;
        this.schema = schema;

        updateLastUsage();
    }

    /**
     * @return Schema name if schema is set, null otherwise.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Connection.
     */
    public Connection connection() {
        return conn;
    }

    /**
     * @return Statement cache.
     */
    public H2StatementCache statementCache() {
        return stmtCache;
    }

    /**
     * The timestamp of the last usage of the connection.
     *
     * @return last usage timestamp
     */
    public long lastUsage() {
        return lastUsage;
    }

    /**
     * Updates the {@link #lastUsage} timestamp by current time.
     */
    public void updateLastUsage() {
        lastUsage = U.currentTimeMillis();
    }

    /**
     * Closes cached statements.
     */
    public void closeCachedStatements() {
        for (PreparedStatement stmt: stmtCache.values())
            U.closeQuiet(stmt);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        closeCachedStatements();

        try {
            conn.close();
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2ConnectionWrapper.class, this);
    }
}
