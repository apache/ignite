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
import java.sql.SQLException;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Wrapper to store connection and flag is schema set or not.
 * TODO I suggest that we merge this with statements cache to manage their lifecycles as a whole,
 * currently they are separated.
 * Also, we would not lose all of cached statements
 * if we used connection, its schema and associated statements all together.
 */
class H2ConnectionWrapper implements Closeable {
    /** */
    private final Connection conn;

    /** */
    private final String schema;

    /** Cache. */
    private final H2ConnectionCache cache;

    /**
     * @param conn Connection to use.
     * @param schema Schema name.
     * @param cache Cache.
     */
    H2ConnectionWrapper(Connection conn, String schema, H2ConnectionCache cache) {
        this.conn = conn;
        this.schema = schema;
        this.cache = cache;

        cache.put(schema, this);
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2ConnectionWrapper.class, this);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        try {
            conn.close();
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }

        cache.remove(schema);
    }
}
