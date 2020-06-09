/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql.runner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Simple JDBC connection pool for testing.
 */
public class SimpleConnectionPool implements AutoCloseable {
    /** */
    private final List<Connection> connPool;

    /** */
    private final List<Connection> usedConnections;

    /** */
    public SimpleConnectionPool(String url, int port, int size) throws SQLException {
        connPool = new ArrayList<>(size);
        usedConnections = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            Connection conn = DriverManager.getConnection(url + port);

            conn.setSchema("PUBLIC");

            connPool.add(conn);
        }
    }

    /** */
    public synchronized Connection getConnection() {
        Connection conn = connPool.remove(connPool.size() - 1);

        usedConnections.add(conn);

        return conn;
    }

    /** */
    public synchronized boolean releaseConnection(Connection conn) {
        connPool.add(conn);

        return usedConnections.remove(conn);
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        connPool.forEach(U::closeQuiet);
        usedConnections.forEach(U::closeQuiet);
    }
}
