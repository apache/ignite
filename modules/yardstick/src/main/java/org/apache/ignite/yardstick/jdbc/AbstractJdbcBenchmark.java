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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.jdbc.thin.JdbcThinUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.apache.ignite.yardstick.jdbc.JdbcUtils.fillData;
import static org.yardstickframework.BenchmarkUtils.println;

/**
 * JDBC benchmark that performs query operations.
 */
public abstract class AbstractJdbcBenchmark extends IgniteAbstractBenchmark {
    /** All {@link Connection}s associated with threads. */
    private final List<Connection> threadConnections = new ArrayList<>();

    /** JDBC URL. */
    protected String url;

    /** Each connection is also a transaction, so we better pin them to threads. */
    protected ThreadLocal<Connection> conn = new ThreadLocal<Connection>() {
        @Override protected Connection initialValue() {
            try {
                Connection conn = connection(url);

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

        // activate cluster if it is not auto activated
        ignite().cluster().active(true);

        if (url == null) {
            if (args.jdbcUrl().startsWith(JdbcThinUtils.URL_PREFIX)) {
                String addr = findThinAddress();
                url = JdbcThinUtils.URL_PREFIX + addr + '/';
            }
            else
                url = args.jdbcUrl();
        }

        println("Using jdbc url:" + url);

        setupData();

        ignite().close();
    }

    /**
     * Sets up test data
     *
     * Gets executed before local Ignite node is closed
     * @throws Exception On error.
     */
    protected void setupData() throws Exception {
        fillData(cfg, (IgniteEx)ignite(), args.range(), args.atomicMode());
    }

    /**
     * Find address of client node, that thin driver should use.
     *
     * @return Address for thin driver.
     */
    private String findThinAddress(){
        for (ClusterNode n : ignite().cluster().forClients().nodes()) {
            if (n.isLocal())
                continue;

            // try to find non-localhost address of this node
            for (String addr : n.addresses()) {
                if (!addr.equals("127.0.0.1")
                    && !addr.equals("localhost")
                    && !addr.equals("172.17.0.1")) {

                    println("Found remote node: " + addr);
                    return addr;
                }
            }

            // otherwise this node is running on localhost in a separate jvm
            println("Found another client node on localhost");
            return "127.0.0.1";
        }

        throw new RuntimeException("Setup exception: could not find non-local node, check your setup");
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
     * Create new {@link Connection} from {@link #args}. Intended for use by {@link #setUp} and {@link #tearDown}.
     * @return JDBC connection.
     * @throws SQLException On error.
     */
    protected final Connection connection(String url) throws SQLException {
        println("JDBC connect to: " + url);

        Connection conn = DriverManager.getConnection(url);

        conn.setSchema("PUBLIC");

        return conn;
    }

    /**
     * Create thread local prepared statement.
     * @param sql - sql query for statement.
     * @return Prepared statement.
     */
    final ThreadLocal<PreparedStatement> newStatement(final String sql){
        return new ThreadLocal<PreparedStatement>(){
            @Override protected PreparedStatement initialValue() {
                try {
                    return conn.get().prepareStatement(sql);
                }
                catch (SQLException e) {
                    throw new IgniteException(e);
                }
            }
        };
    }
}
