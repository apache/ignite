/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper to store connection with currently used schema and statement cache.
 */
public class H2ConnectionWrapper implements AutoCloseable {
    /** */
    private static final int STATEMENT_CACHE_SIZE = 256;

    /** */
    private final Connection conn;

    /** */
    private volatile String schema;

    /** */
    private volatile H2StatementCache statementCache;

    /**
     * @param conn Connection to use.
     */
    H2ConnectionWrapper(Connection conn) {
        this.conn = conn;

        initStatementCache();
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
    public void schema(@Nullable String schema) {
        this.schema = schema;
    }

    /**
     * Connection for schema.
     *
     * @param schema Schema name.
     * @return Connection.
     */
    public Connection connection(@Nullable String schema) {
        if (schema != null && !F.eq(this.schema, schema)) {
            try {
                conn.setSchema(schema);

                this.schema = schema;
            }
            catch (SQLException e) {
                throw new IgniteSQLException("Failed to set schema for DB connection for thread [schema=" +
                    schema + "]", e);
            }
        }

        return conn;
    }

    /**
     * @return Connection.
     */
    public Connection connection() {
        return conn;
    }

    /**
     * @return Statement cache corresponding to connection.
     */
    public H2StatementCache statementCache() {
        return statementCache;
    }

    /**
     * Clears statement cache.
     */
    public void clearStatementCache() {
        initStatementCache();
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2ConnectionWrapper.class, this);
    }

    /** Closes wrapped connection. */
    @Override public void close() {
        U.closeQuiet(conn);
    }
}
