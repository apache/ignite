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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteException;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/** JDBC benchmark that performs raw SQL insert */
public class RdbmsBenchmark extends JdbcAbstractBenchmark {
    /** Default number of rows in Accounts table. */
    private long accRows;

    /** Default number of rows in Tellers table. */
    private long tellRows;

    /** Default number of rows in Branches table. */
    private long branchRows;

    /** Id for History table */
    private AtomicLong cnt;

    /** Flag for working with Ignite jdbc. */
    private boolean isIgnite;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        isIgnite = conn.get().getMetaData().getDatabaseProductName().equals("Apache Ignite");

        if (isIgnite)
            clearCaches();
        else {
            conn.get().createStatement().execute("DROP TABLE if exists History;");
            conn.get().createStatement().execute("CREATE TABLE if not exists History (id BIGINT, aid BIGINT, tid BIGINT, bid BIGINT, delta BIGINT);");
        }
        cnt = new AtomicLong();

        accRows = 1000L * args.scaleFactor();

        tellRows = 10L * args.scaleFactor();

        branchRows = 5L * args.scaleFactor();

        fillTable("Accounts", accRows);

        fillTable("Tellers", tellRows);

        fillTable("Branches", branchRows);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long aid = ThreadLocalRandom.current().nextLong(accRows);
        long bid = ThreadLocalRandom.current().nextLong(branchRows);
        long tid = ThreadLocalRandom.current().nextLong(tellRows);

        long delta = ThreadLocalRandom.current().nextLong(1000);

        for (String query : getDbqueries()) {
            boolean done = false;

            query = query.replaceAll(":aid", Long.toString(aid));
            query = query.replaceAll(":bid", Long.toString(bid));
            query = query.replaceAll(":tid", Long.toString(tid));
            query = query.replaceAll(":delta", Long.toString(delta));
            if (query.contains(":id"))
                query = query.replaceAll(":id", Long.toString(cnt.getAndIncrement()));

            try (PreparedStatement stmt = conn.get().prepareStatement(query)) {
                if (isIgnite && query.startsWith("INSERT")) {
                    stmt.setLong(1, cnt.getAndIncrement());
                    stmt.setLong(2, tid);
                    stmt.setLong(3, bid);
                    stmt.setLong(4, aid);
                    stmt.setLong(5, delta);
                }
                while (!done) {
                    stmt.execute();
                    done = true;
                }
            }
            catch (Exception ignored) {
                BenchmarkUtils.println("Failed to execute query " + query);
            }
        }
        return true;
    }

    /**
     * Create upsert statement depending on the type of database
     *
     * @param conn {@link Connection} to get metadata from to determine storage type
     * @param tblName Target table name.
     * @return upsert statement with params set
     * @throws SQLException if failed
     */

    private static PreparedStatement createUpsertStatement(Connection conn, String tblName) throws SQLException {
        switch (conn.getMetaData().getDatabaseProductName()) {
            case "H2":
                return conn.prepareStatement("merge into " + tblName + " (id, val) values(?, ?)");

            case "Apache Ignite":
                return conn.prepareStatement("merge into " + '"' + tblName + '"' + '.' + tblName + " (_key, val) values(?, ?)");

            case "MySQL":
                return conn.prepareStatement("insert into " + tblName + " (id, val) values(?, ?) on duplicate key " +
                    "update val = ?");

            case "PostgreSQL":
                return conn.prepareStatement("insert into " + tblName + " (id, val) values(?, ?) on conflict(id) do " +
                    "update set val = ?");

            default:
                throw new IgniteException("Unexpected database type [databaseProductName=" +
                    conn.getMetaData().getDatabaseProductName() + ']');
        }
    }

    /**
     * Set args to prepared upsert statement.
     *
     * @param stmt Statement.
     * @param newKey Key.
     * @param newVal Value.
     * @throws SQLException if failed.
     */
    private static void setUpsertStatementArgs(PreparedStatement stmt, long newKey, long newVal)
        throws SQLException {
        switch (stmt.getConnection().getMetaData().getDatabaseProductName()) {
            case "H2":
                // No-op.
                break;

            case "Apache Ignite":
                // No-op.
                break;

            case "MySQL":
            case "PostgreSQL":
                stmt.setLong(3, newVal);

                break;

            default:
                throw new IgniteException("Unexpected database type [databaseProductName=" +
                    stmt.getConnection().getMetaData().getDatabaseProductName() + ']');
        }
        stmt.setLong(1, newKey);
        stmt.setLong(2, newVal);
    }

    /**
     * Fill tables with random data.
     *
     * @param tblName name of the table to fill
     * @param rows number of rows in the table
     * @throws Exception if failed.
     */
    private void fillTable(String tblName, long rows) throws Exception {
        if (!isIgnite && args.schemaDefinition() == null)
            return;

        if (isIgnite)
            startPreloadLogging(args.preloadLogsInterval());

        try (PreparedStatement stmt = createUpsertStatement(conn.get(), tblName)) {
            for (long i = 0; i < rows; i++) {
                int newVal = nextRandom(args.range());

                setUpsertStatementArgs(stmt, i, newVal);

                stmt.execute();

                if (i % 1000 == 0)
                    BenchmarkUtils.println("Inserting " + i + "th value into " + tblName);
            }
        }
        stopPreloadLogging();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Clear caches.
     */
    private void clearCaches() {
        ignite().cache("Accounts").clear();
        ignite().cache("Tellers").clear();
        ignite().cache("Branches").clear();
        ignite().cache("History").clear();
    }
}
