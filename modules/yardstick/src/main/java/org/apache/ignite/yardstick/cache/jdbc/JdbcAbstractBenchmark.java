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

package org.apache.ignite.yardstick.cache.jdbc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.IgniteBenchmarkArguments;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.jcommander;

/** Base class for benchmarks that measure raw performance of JDBC databases */
public abstract class JdbcAbstractBenchmark extends IgniteAbstractBenchmark {
    /** Arguments. */
    protected final IgniteBenchmarkArguments args = new IgniteBenchmarkArguments();

    /** All {@link Connection}s associated with threads. */
    private final List<Connection> threadConnections = new ArrayList<>();

    /** List of queries. */
    private final List<String> dbqueries = new ArrayList<>();

    /** Each connection is also a transaction, so we better pin them to threads. */
    ThreadLocal<Connection> conn = new ThreadLocal<Connection>() {
        @Override protected Connection initialValue() {
            Connection conn;
            try {
                conn = connection();
                if (args.createTempDatabase() || args.dbn() != null) {
                    assert dbName != null;
                    conn.setCatalog(dbName);
                }
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
            synchronized (threadConnections) {
                threadConnections.add(conn);
            }
            return conn;
        }
    };

    /** Dynamically generated DB name */
    private String dbName;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
        jcommander(cfg.commandLineArguments(), args, "<ignite-driver>");

        Class.forName(args.jdbcDriver());
        if (args.createTempDatabase())
            createTestDatabase();
        else
            dbName = (args.dbn() == null) ? null : args.dbn();

        try (Connection conn = connection()) {
            if (args.createTempDatabase())
                conn.setCatalog(dbName);

            populateTestDatabase(conn);
        }

        if (args.loadTestQueriesFile() != null)
            setQueries();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        synchronized (threadConnections) {
            for (Connection conn : threadConnections)
                U.closeQuiet(conn);

            threadConnections.clear();
        }

        if (args.createTempDatabase())
            dropTestDatabase();

        super.tearDown();
    }

    /** Create new, randomly named database to put dummy benchmark data in - no need in this for databases like H2 */
    private void createTestDatabase() throws SQLException {
        try (Connection conn = connection()) {
            String uuid = UUID.randomUUID().toString().replace("-", "");
            dbName = "benchmark" + getClass().getSimpleName() + uuid.substring(0, 16);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("create database " + dbName);
            }
        }
    }

    /** Drop previously created randomly named database - no need in this for databases like H2 */
    private void dropTestDatabase() throws SQLException {
        try (Connection conn = connection()) {
            try (Statement stmt = conn.createStatement()) {
                if (!conn.getMetaData().getDatabaseProductName().equals("Apache Ignite"))
                    stmt.executeUpdate("drop database " + dbName);
            }
        }
    }

    /** Create benchmark DB schema from supplied SQL file path */
    private void populateTestDatabase(Connection conn) throws IOException, SQLException {
        assert conn != null;
        List<String> queries = new ArrayList<>();
        if (args.schemaDefinition() != null) {
            try (FileReader fr = new FileReader(args.schemaDefinition())) {
                try (BufferedReader br = new BufferedReader(fr)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.trim().isEmpty())
                            continue;

                        queries.add(line.trim());
                    }
                }
            }
        }

        for (String query : queries) {
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.executeUpdate();
            }
        }
    }

    /** Set list of queries defined in file */
    private void setQueries() throws IOException, SQLException {
        if (args.loadTestQueriesFile() != null) {
            try (FileReader fr = new FileReader(args.loadTestQueriesFile())) {
                try (BufferedReader br = new BufferedReader(fr)) {
                    String line;

                    while ((line = br.readLine()) != null) {
                        if (line.trim().isEmpty())
                            continue;

                        dbqueries.add(line.trim());
                    }
                }
            }
        }
    }

    /** Create new {@link Connection} from {@link #args}. Intended for use by {@link #setUp} and {@link #tearDown}  */
    public Connection connection() throws SQLException {
        Connection conn = DriverManager.getConnection(args.jdbcUrl());
        conn.setAutoCommit(true);
        return conn;
    }

    /**
     * @return List of queries.
     */
    public List<String> getDbqueries() {
        return dbqueries;
    }

    /**
     * Delete all data from the table specified
     * @param tblName target table
     * @throws SQLException if failed
     */
    void clearTable(String tblName) throws SQLException {
        try (Connection conn = connection()) {
            try (PreparedStatement stmt = conn.prepareStatement("drop table " + tblName)) {
                if (!conn.getMetaData().getDatabaseProductName().equals("Apache Ignite"))
                    stmt.executeUpdate();
            }
        }
    }
}
