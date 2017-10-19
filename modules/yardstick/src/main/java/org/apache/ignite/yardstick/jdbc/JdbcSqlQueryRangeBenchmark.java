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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.jdbc.thin.JdbcThinUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * JDBC benchmark that performs query operations
 */
public class JdbcSqlQueryRangeBenchmark extends IgniteAbstractBenchmark {
    /** All {@link Connection}s associated with threads. */
    private final List<Connection> threadConnections = new ArrayList<>();

    /** JDBC URL. */
    private String url;

    /** Each connection is also a transaction, so we better pin them to threads. */
    private ThreadLocal<Connection> conn = new ThreadLocal<Connection>() {
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

    /** Statement with range. */
    private ThreadLocal<PreparedStatement> stmtRange = new ThreadLocal<PreparedStatement>() {
        @Override protected PreparedStatement initialValue() {
            try {
                return conn.get().prepareStatement("select id, val from test_long where id > ? and id <= ?");
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
        }
    };

    /** Statement full scan. */
    private ThreadLocal<PreparedStatement> stmtFull = new ThreadLocal<PreparedStatement>() {
        @Override protected PreparedStatement initialValue() {
            try {
                return conn.get().prepareStatement("select id, val from test_long");
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
        }
    };

    /** Statement full scan. */
    private ThreadLocal<PreparedStatement> stmtSingle = new ThreadLocal<PreparedStatement>() {
        @Override protected PreparedStatement initialValue() {
            try {
                return conn.get().prepareStatement("select id, val from test_long where id = ?");
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
        }
    };

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        println(cfg, "Create table...");

        try (Statement stmt = conn.get().createStatement()) {
            stmt.executeUpdate("CREATE TABLE test_long (id long primary key, val long)");

            try (PreparedStatement pstmt = conn.get().prepareStatement("insert into test_long (id, val) values (?, ?)")) {
                println(cfg, "Populate data...");

                for (long l = 1; l <= args.keysCount(); ++l) {
                    pstmt.setLong(1, l);
                    pstmt.setLong(2, l + 1);

                    pstmt.executeUpdate();

                    if (l % 10000 == 0)
                        println(cfg, "Populate " + l);
                }
            }
        }

        println(cfg, "Finished populating data");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long expRsSize;

        PreparedStatement stmt;

        if (args.resultSetSize() <= 0) {
            stmt = stmtFull.get();

            expRsSize = args.keysCount();
        }
        else if (args.resultSetSize() == 1) {
            stmt = stmtSingle.get();

            stmt.setLong(1, ThreadLocalRandom.current().nextLong(args.keysCount() + 1));

            expRsSize = 1;
        }
        else {
            stmt = stmtRange.get();

            long id = ThreadLocalRandom.current().nextLong(args.keysCount() - args.range());
            long maxId = id + args.resultSetSize();

            stmt.setLong(1, id);
            stmt.setLong(2, maxId);

            expRsSize = args.resultSetSize();
        }

        long rsSize = 0;

        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                if (rs.getLong(1) + 1 != rs.getLong(2))
                    throw new Exception("Invalid result retrieved");

                rsSize++;
            }
        }

        if (rsSize != expRsSize)
            throw new Exception("Invalid result set size [rsSize=" + rsSize + ", expected=" + expRsSize + ']');

        return true;
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
        synchronized (url) {
            if (url == null) {
                url = args.jdbcUrl();

                if (args.jdbcUrl().startsWith(JdbcThinUtils.URL_PREFIX)) {
                    for (ClusterNode n : ignite().cluster().forClients().nodes()) {
                        if (!n.isLocal())
                            url = JdbcThinUtils.URL_PREFIX + n.addresses().iterator().next();
                    }
                }

                // Stop local node after discover remote node IP.
                Ignition.stop(ignite().name(), false);
            }
        }

        println("JDBC connect to: " + url);

        return DriverManager.getConnection(url);
    }
}
