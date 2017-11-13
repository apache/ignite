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

package org.apache.ignite.yardstick.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.jdbc.thin.JdbcThinUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * JDBC benchmark that performs query operations
 */
abstract public class AbstractJdbcBenchmark extends IgniteAbstractBenchmark {
    /** All {@link Connection}s associated with threads. */
    private final List<Connection> threadConnections = new ArrayList<>();

    /** JDBC URL. */
    private String url;

    /** Each connection is also a transaction, so we better pin them to threads. */
    protected ThreadLocal<Connection> conn = new ThreadLocal<Connection>() {
        @Override protected Connection initialValue() {
            try {
                Connection conn = connection();

                synchronized (threadConnections) {
                    threadConnections.add(conn);
                }

                return conn;
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
        }
    };

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        if (url == null) {
            if (args.jdbcUrl().startsWith(JdbcThinUtils.URL_PREFIX)) {
                for (ClusterNode n : ignite().cluster().forClients().nodes()) {
                    if (!n.isLocal()) {
                        for (String addr : n.addresses()) {
                            if (!addr.equals("127.0.0.1") && !addr.equals("localhost")
                                && !addr.equals("172.17.0.1")) {
                                url = JdbcThinUtils.URL_PREFIX + addr + '/';

                                println("Found remote node: " + addr);

                                break;
                            }
                        }

                        if (url != null)
                            break;
                    }
                }
            }
            else
                url = args.jdbcUrl();
        }
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        synchronized (threadConnections) {
            for (Connection conn : threadConnections)
                U.closeQuiet(conn);

            threadConnections.clear();
        }

        super.tearDown();
    }

    /**
     * Create new {@link Connection} from {@link #args}. Intended for use by {@link #setUp} and {@link #tearDown}
     * @return JDBC connection.
     * @throws SQLException On error.
     */
    private Connection connection() throws SQLException {
        println("JDBC connect to: " + url);

        Connection conn = DriverManager.getConnection(url);

        conn.setSchema("PUBLIC");

        return conn;
    }
}
