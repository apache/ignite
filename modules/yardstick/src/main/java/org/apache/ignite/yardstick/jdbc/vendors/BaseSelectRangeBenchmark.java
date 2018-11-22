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

package org.apache.ignite.yardstick.jdbc.vendors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.yardstick.jdbc.AbstractJdbcBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Abstract benchmark for sql select operation, that has range in WHERE clause.Designed to compare Ignite and
 * other DBMSes. Children specify what exactly query gets executed.
 */
public abstract class BaseSelectRangeBenchmark extends AbstractJdbcBenchmark {
    /** Factory that hides all sql queries. */
    protected QueryFactory queries;

    /** Number of persons in that times greater than organizations. */
    private static final int ORG_TO_PERS_FACTOR = 10;

    /** Batch size */
    private static final int BATCH_SIZE = 10_000;

    /** Resources that should be closed. For example, opened thread local statements. */
    private List<AutoCloseable> toClose = Collections.synchronizedList(new ArrayList<>());

    /** Thread local select statement. */
    protected ThreadLocal<PreparedStatement> select = new ThreadLocal<PreparedStatement>() {
        @Override protected PreparedStatement initialValue() {
            try {
                Connection locConn = conn.get();

                PreparedStatement sel = locConn.prepareStatement(testedSqlQuery());

                toClose.add(sel);

                return sel;
            }
            catch (SQLException e) {
                throw new RuntimeException("Can't create thread local statement.", e);
            }
        }
    };

    /**
     * Children implement this method to specify what statement to prepare.
     * During benchmark run, this prepared statement gets executed with random parameters:
     * minimum and maximum values for salary field (in WHERE clause).
     *
     * @return sql query with 2 parameters.
     */
    protected abstract String testedSqlQuery();

    /** {@inheritDoc} */
    @Override protected void setupData() throws Exception {
        // Don't use default tables.
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        queries = new QueryFactory();

        executeUpdate(conn.get(), queries.dropPersonIfExist());
        executeUpdate(conn.get(), queries.dropOrgIfExist());

        executeUpdate(conn.get(), queries.createPersonTab());
        executeUpdate(conn.get(), queries.createOrgTab());

        executeUpdate(conn.get(), queries.beforeLoad());

        conn.get().setAutoCommit(false);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long orgRng = args.range() / ORG_TO_PERS_FACTOR;

        println(cfg, "Populating Organization table.");

        try (PreparedStatement insOrg = conn.get().prepareStatement(queries.insertIntoOrganization())) {

            long percent = 0;

            for (long orgId = 0; orgId < orgRng; orgId++) {
                insOrg.setLong(1, orgId);
                insOrg.setString(2, "organization#" + orgId);

                insOrg.addBatch();

                if ((orgId +1) % BATCH_SIZE == 0) {
                    insOrg.executeBatch();


                    long newPercent = (orgId + 1) * 100 / orgRng;

                    if (percent != newPercent) {
                        percent = newPercent;

                        println(cfg, (orgId + 1) + " out of " + orgRng + " rows have been uploaded " +
                            "(" + percent + "%).");
                    }
                }
            }
        }

        println(cfg, "Populating Person table.");

        try (PreparedStatement insPers = conn.get().prepareStatement(queries.insertIntoPerson())) {
            long percent = 0;

            for (long persId = 0; persId < args.range(); persId++) {
                long orgId = rnd.nextLong(orgRng);

                fillPersonArgs(insPers, persId, orgId).addBatch();

                if ((persId + 1) % BATCH_SIZE == 0) {
                    insPers.executeBatch();

                    long newPercent = (persId + 1) * 100 / args.range();

                    if (percent != newPercent) {
                        percent = newPercent;

                        println(cfg, (persId+1) + " out of " + args.range() + " rows have been uploaded " +
                            "(" + percent + "%).");
                    }
                }
            }
        }

        conn.get().setAutoCommit(true);

        executeUpdate(conn.get(), queries.afterLoad());

        println(cfg, "Database have been populated.");
    }

    /**
     * Perform sql update operation on specified jdbc connection.
     *
     * @param c jdbc connection to use.
     * @param sql text of the update query.
     * @throws SQLException on error.
     */
    static void executeUpdate(Connection c, String sql) throws SQLException {
        if (F.isEmpty(sql))
            return;

        try (Statement upd = c.createStatement()) {
            upd.executeUpdate(sql);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        PreparedStatement select0 = select.get();

        long minSalary = ThreadLocalRandom.current().nextLong(args.range() - args.sqlRange() + 1);
        long maxSalary = minSalary + args.sqlRange() - 1;

        select0.setLong(1, minSalary * 1000);
        select0.setLong(2, maxSalary * 1000);

        long rsCnt = 0;

        try (ResultSet res = select0.executeQuery()) {
            while (res.next())
                rsCnt++;
        }

        if (rsCnt != args.sqlRange())
            throw new AssertionError("Server returned wrong number of lines: " +
                "[expected=" + args.sqlRange() + ", actual=" + rsCnt + "].");

        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        try {
            executeUpdate(conn.get(), queries.dropPersonIfExist());
            executeUpdate(conn.get(), queries.dropOrgIfExist());

            for (AutoCloseable stmt : toClose)
                stmt.close();
        }
        finally {
            super.tearDown();
        }
    }

    /**
     * Generate Person record and fill with it specified statement.
     *
     * @param insPers PreparedStatement which parametets to fill.
     * @param id person's id.
     * @param orgId id of organization, this person associated with.
     */
    static PreparedStatement fillPersonArgs(PreparedStatement insPers, long id, long orgId) throws SQLException {
        insPers.setLong(1, id);
        insPers.setLong(2, orgId);
        insPers.setString(3, "firstName" + id);
        insPers.setString(4, "lastName" + id);
        insPers.setLong(5, id * 1000);

        return insPers;
    }
}
