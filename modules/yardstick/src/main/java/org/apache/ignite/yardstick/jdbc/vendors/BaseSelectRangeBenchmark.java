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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.yardstick.IgniteBenchmarkArguments;
import org.apache.ignite.yardstick.jdbc.AbstractJdbcBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Base benchmark for sql select operation. Designed to compare Ignite and other DBMSes. Children specify what exactly
 * query gets executed and how parameters are filled.
 */
public abstract class BaseSelectRangeBenchmark extends AbstractJdbcBenchmark {
    /** Factory that hides all sql queries. */
    protected QueryFactory queries;

    /** Number of persons in that times greater than organizations. */
    private static final int ORG_TO_PERS_FACTOR = 10;

    /** Size in rows of jdbc batch. Used during database population. */
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
     * Children implement this method to specify what statement to prepare. During benchmark run, this prepared
     * statement gets executed. Parameters are filled by {@link #fillTestedQueryParams(PreparedStatement)} method.
     *
     * @return sql query. Possibly with parameters.
     */
    protected abstract String testedSqlQuery();

    /**
     * Children implement this method to specify how to generate parameters of tested query.
     *
     * See {@link #testedSqlQuery()}.
     */
    protected abstract void fillTestedQueryParams(PreparedStatement select) throws SQLException;

    /** {@inheritDoc} */
    @Override protected void setupData() throws Exception {
        // Don't use default tables.
        // Instead we are able to use ignite instance to check cluster, before this instance gets closed.
         Collection<ClusterNode> srvNodes = ignite().cluster().forServers().nodes();

        if (srvNodes.size() > 1) {
            throw new IllegalStateException("This benchmark is designed to no more than one server (data) node. " +
                "Currently cluster contains " + srvNodes.size() + " server nodes : " + srvNodes);
        }
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        queries = new QueryFactory();

        Connection conn0 = conn.get();

        executeUpdate(conn0, queries.dropPersonIfExist());
        executeUpdate(conn0, queries.dropOrgIfExist());

        executeUpdate(conn0, queries.createPersonTab());
        executeUpdate(conn0, queries.createOrgTab());

        executeUpdate(conn0, queries.createSalaryIdx());
        executeUpdate(conn0, queries.createOrgIdIdx());

        executeUpdate(conn0, queries.beforeLoad());

        boolean oldAutoCommit = conn0.getAutoCommit();

        trySetAutoCommit(conn0, false);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long orgRng = args.range() / ORG_TO_PERS_FACTOR;

        println(cfg, "Populating Organization table.");

        try (PreparedStatement insOrg = conn0.prepareStatement(queries.insertIntoOrganization())) {
            long percent = 0;

            for (long orgId = 0; orgId < orgRng; orgId++) {
                insOrg.setLong(1, orgId);
                insOrg.setString(2, "organization#" + orgId);

                insOrg.addBatch();

                if ((orgId + 1) % BATCH_SIZE == 0 || (orgId + 1) == orgRng) {
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

        try (PreparedStatement insPers = conn0.prepareStatement(queries.insertIntoPerson())) {
            long percent = 0;

            for (long persId = 0; persId < args.range(); persId++) {
                long orgId = rnd.nextLong(orgRng);

                fillPersonArgs(insPers, persId, orgId).addBatch();

                if ((persId + 1) % BATCH_SIZE == 0) {
                    insPers.executeBatch();

                    long newPercent = (persId + 1) * 100 / args.range();

                    if (percent != newPercent) {
                        percent = newPercent;

                        println(cfg, (persId + 1) + " out of " + args.range() + " rows have been uploaded " +
                            "(" + percent + "%).");
                    }
                }
            }
        }

        trySetAutoCommit(conn0, oldAutoCommit);

        executeUpdate(conn0, queries.afterLoad());

        println(cfg, "Database have been populated.");

        String explainSql = "EXPLAIN " + testedSqlQuery();

        try (PreparedStatement expStat = conn0.prepareStatement(explainSql);) {
            fillTestedQueryParams(expStat);

            try (ResultSet explain = expStat.executeQuery()) {
                println(cfg, "Explain query " + explainSql + " result:");

                println(cfg, tableToString(explain));
            }
        }
    }

    /**
     * Prints result set as formatted table.
     *
     * @param rs result set that represets table.
     */
    private static String tableToString(ResultSet rs) throws SQLException {
        StringBuilder buf = new StringBuilder();

        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            String cell = String.format("%-20s", rs.getMetaData().getColumnName(i));

            buf.append(cell).append("\t");
        }

        buf.append("\n");

        while (rs.next()) {
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                String cell = String.format("%-20s", rs.getString(i));

                buf.append(cell).append("\t");
            }

            buf.append("\t");
        }

        return buf.toString();
    }

    /**
     * Set auto commit if it is supported. Log warning otherwise.
     *
     * @param conn Set auto commit mode to this connection.
     * @param autocommit Autocommit mode parameter value.
     */
    private void trySetAutoCommit(Connection conn, boolean autocommit) throws SQLException {
        try {
            conn.setAutoCommit(autocommit);
        }
        catch (SQLFeatureNotSupportedException ignored) {
            println(cfg, "Failed to set auto commit to " + autocommit + " because it is unsupported operation, " +
                "will just ignore it.");
        }
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

    /**
     * Given query that has 2 parameters which should be values of salary, according to the data model. This method
     * generates range with a random begin point and fixed width({@link IgniteBenchmarkArguments#sqlRange()}) and fills
     * prepared statement, that represents such query, with values : range begin and range end.
     *
     * @param selectBySalary Prepared statement, representing select with filter by salary field.
     */
    protected void fillRandomSalaryRange(PreparedStatement selectBySalary) throws SQLException {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long minId = rnd.nextLong(args.range() - args.sqlRange() + 1);
        long maxId = minId + args.sqlRange() - 1;

        long minSalary = minId * 1000;
        long maxSalary = maxId * 1000;

        selectBySalary.setLong(1, minSalary);
        selectBySalary.setLong(2, maxSalary);
    }

    /**
     * Template method for benchmark. Executes thread-local  PreparedStatement. Children are specifying what query with
     * what parameters to execute and how to fill it's parameters.
     */
    @Override public final boolean test(Map<Object, Object> map) throws Exception {
        PreparedStatement select0 = select.get();

        fillTestedQueryParams(select0);

        try (ResultSet res = select0.executeQuery()) {
            readResults(res);
        }

        return true;
    }

    /**
     * Reads all the specified result set. Checks that result size is as expected.
     *
     * @param qryRes result set of executed select.
     */
    private void readResults(ResultSet qryRes) throws SQLException {
        long rsCnt = 0;

        while (qryRes.next())
            rsCnt++;

        if (rsCnt != args.sqlRange())
            throw new AssertionError("Server returned wrong number of lines: " +
                "[expected=" + args.sqlRange() + ", actual=" + rsCnt + "].");
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
