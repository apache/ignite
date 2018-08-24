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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 * Base class for complex SQL tests based on JDBC driver.
 */
public class JdbcThinComplexDmlDdlSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache mode to test with. */
    private final CacheMode cacheMode = CacheMode.PARTITIONED;

    /** Cache atomicity mode to test with. */
    private final CacheAtomicityMode atomicityMode = CacheAtomicityMode.ATOMIC;

    /** Names of companies to use. */
    private static final List<String> COMPANIES = Arrays.asList("ASF", "GNU", "BSD");

    /** Cities to use. */
    private static final List<String> CITIES = Arrays.asList("St. Petersburg", "Boston", "Berkeley", "London");

    /** JDBC connection. */
    private Connection conn;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(@NotNull String name) {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);

        return cfg;
    }

    /**
     * @return JDBC connection.
     * @throws SQLException On error.
     */
    protected Connection createConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        conn.close();

        // Destroy all SQL caches after test.
        for (String cacheName : grid(0).cacheNames()) {
            DynamicCacheDescriptor cacheDesc = grid(0).context().cache().cacheDescriptor(cacheName);

            if (cacheDesc != null && cacheDesc.sql())
                grid(0).destroyCache0(cacheName, true);
        }

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testCreateSelectDrop() throws Exception {
        conn = createConnection();

        sql(new UpdateChecker(0),
            "CREATE TABLE person (id int, name varchar, age int, company varchar, city varchar, " +
                "primary key (id, name, city)) WITH \"template=" + cacheMode.name() + ",atomicity=" + atomicityMode.name()
                + ",affinity_key=city\"");

        sql(new UpdateChecker(0), "CREATE INDEX idx on person (city asc, name asc)");

        sql(new UpdateChecker(0), "CREATE TABLE city (name varchar, population int, primary key (name)) WITH " +
            "\"template=" + cacheMode.name() + ",atomicity=" + atomicityMode.name() + ",affinity_key=name\"");

        sql(new UpdateChecker(3),
            "INSERT INTO city (name, population) values(?, ?), (?, ?), (?, ?)",
            "St. Petersburg", 6000000,
            "Boston", 2000000,
            "London", 8000000
        );

        sql(new ResultColumnChecker("id", "name", "age", "comp"),
            "SELECT id, name, age, company as comp FROM person where id < 50");

        for (int i = 0; i < 100; i++) {
            sql(new UpdateChecker(1),
                "INSERT INTO person (id, name, age, company, city) values (?, ?, ?, ?, ?)",
                i,
                "Person " + i,
                20 + (i % 10),
                COMPANIES.get(i % COMPANIES.size()),
                CITIES.get(i % CITIES.size()));
        }

        final int[] cnt = {0};

        sql(new ResultPredicateChecker(new IgnitePredicate<Object[]>() {
            @Override public boolean apply(Object[] objs) {
                int id = ((Integer)objs[0]);

                if (id >= 50)
                    return false;

                if (20 + (id % 10) != ((Integer)objs[2]))
                    return false;

                if (!("Person " + id).equals(objs[1]))
                    return false;

                ++cnt[0];

                return true;
            }
        }), "SELECT id, name, age FROM person where id < 50");

        assert cnt[0] == 50 : "Invalid rows count";

        // Berkeley is not present in City table, although 25 people have it specified as their city.
        sql(new ResultChecker(new Object[][] {{75L}}),
            "SELECT COUNT(*) from Person p inner join City c on p.city = c.name");

        sql(new UpdateChecker(34),
            "UPDATE Person SET company = 'New Company', age = CASE WHEN MOD(id, 2) <> 0 THEN age + 5 ELSE "
                + "age + 1 END WHERE company = 'ASF'");

        cnt[0] = 0;

        sql(new ResultPredicateChecker(new IgnitePredicate<Object[]>() {
            @Override public boolean apply(Object[] objs) {
                int id = ((Integer)objs[0]);
                int age = ((Integer)objs[2]);

                if (id % 2 == 0) {
                    if (age != 20 + (id % 10) + 1)
                        return false;
                }
                else {
                    if (age != 20 + (id % 10) + 5)
                        return false;
                }

                ++cnt[0];

                return true;
            }
        }), "SELECT * FROM person where company = 'New Company'");

        assert cnt[0] == 34 : "Invalid rows count";

        sql(new UpdateChecker(0), "DROP INDEX idx");

        sql(new UpdateChecker(0), "DROP TABLE city");
        sql(new UpdateChecker(0), "DROP TABLE person");
    }

    /**
     * Run sql statement with arguments and check results.
     *
     * @param checker Query result's checker.
     * @param sql SQL statement to execute.
     * @param args Arguments.
     *
     * @throws SQLException On failed.
     */
    protected void sql(SingleStatementChecker checker, String sql, Object... args) throws SQLException {
        Statement stmt;

        if (args.length > 0) {
            stmt = conn.prepareStatement(sql);

            PreparedStatement pstmt = (PreparedStatement)stmt;

            for (int i = 0; i < args.length; ++i)
                pstmt.setObject(i + 1, args[i]);

            pstmt.execute();
        }
        else {
            stmt = conn.createStatement();

            stmt.execute(sql);
        }

        checkResults(stmt, checker);
    }

    /**
     * Check query results wwith provided checker.
     *
     * @param stmt Statement.
     * @param checker Checker.
     * @throws SQLException If failed.
     */
    private void checkResults(Statement stmt, SingleStatementChecker checker) throws SQLException {
        ResultSet rs = stmt.getResultSet();

        if (rs != null)
            checker.check(rs);
        else {
            int updCnt = stmt.getUpdateCount();

            assert updCnt != -1 : "Invalid results. Result set is null and update count is -1";

            checker.check(updCnt);
        }
    }

    /**
     *
     */
    interface SingleStatementChecker {
        /**
         * Called when query produces results.
         *
         * @param rs Result set.
         * @throws SQLException On error.
         */
        void check(ResultSet rs) throws SQLException;

        /**
         * Called when query produces any update.
         *
         * @param updateCount Update count.
         */
        void check(int updateCount);
    }

    /**
     *
     */
    static class UpdateChecker implements SingleStatementChecker {
        /** Expected update count. */
        private final int expUpdCnt;

        /**
         * @param expUpdCnt Expected Update count.
         */
        UpdateChecker(int expUpdCnt) {
            this.expUpdCnt = expUpdCnt;
        }

        /** {@inheritDoc} */
        @Override public void check(ResultSet rs) {
            fail("Update results are expected. [rs=" + rs + ']');
        }

        /** {@inheritDoc} */
        @Override public void check(int updateCount) {
            assertEquals(expUpdCnt, updateCount);
        }
    }

    /**
     *
     */
    static class ResultChecker implements SingleStatementChecker {
        /** Expected update count. */
        private final Set<Row> expRs = new HashSet<>();

        /**
         * @param expRs Expected result set.
         */
        ResultChecker(Object[][] expRs) {
            for (Object[] row : expRs)
                this.expRs.add(new Row(row));
        }

        /** {@inheritDoc} */
        @Override public void check(ResultSet rs) throws SQLException {
            int cols = rs.getMetaData().getColumnCount();

            while (rs.next()) {
                Object [] rowObjs = new Object[cols];

                for (int i = 0; i < cols; ++i)
                    rowObjs[i] = rs.getObject(i + 1);

                Row row = new Row(rowObjs);

                assert expRs.remove(row) : "Invalid row. [row=" + row + ", remainedRows="
                    + printRemainedExpectedResult() + ']';
            }

            assert expRs.isEmpty() : "Expected results has rows that aren't contained at the result set. [remainedRows="
                + printRemainedExpectedResult() + ']';
        }

        /** {@inheritDoc} */
        @Override public void check(int updateCount) {
            fail("Results set is expected. [updateCount=" + updateCount + ']');
        }

        /**
         * @return Print remaining expected rows.
         */
        private String printRemainedExpectedResult() {
            StringBuilder sb = new StringBuilder();

            for (Row r : expRs)
                sb.append('\n').append(r.toString());

            return sb.toString();
        }
    }

    /**
     *
     */
    static class ResultColumnChecker extends ResultChecker {
        /** Expected column names. */
        private final String[] expColLabels;

        /**
         * Checker column names for rmpty results.
         *
         * @param expColLabels Expected column names.
         */
        ResultColumnChecker(String... expColLabels) {
            super(new Object[][]{});

            this.expColLabels = expColLabels;
        }

        /** {@inheritDoc} */
        @Override public void check(ResultSet rs) throws SQLException {
            ResultSetMetaData meta = rs.getMetaData();

            int cols = meta.getColumnCount();

            assert cols == expColLabels.length : "Invalid columns count: [expected=" + expColLabels.length
                + ", actual=" + cols + ']';

            for (int i = 0; i < cols; ++i)
                assert expColLabels[i].equalsIgnoreCase(meta.getColumnName(i + 1));

            super.check(rs);
        }
    }

    /**
     *
     */
    static class ResultPredicateChecker implements SingleStatementChecker {
        /** Row predicate. */
        private IgnitePredicate<Object[]> rowPredicate;

        /**
         * @param rowPredicate Row predicate to check result set.
         */
        ResultPredicateChecker(IgnitePredicate<Object[]> rowPredicate) {
            this.rowPredicate = rowPredicate;
        }

        /** {@inheritDoc} */
        @Override public void check(ResultSet rs) throws SQLException {
            int cols = rs.getMetaData().getColumnCount();

            while (rs.next()) {
                Object [] rowObjs = new Object[cols];

                for (int i = 0; i < cols; ++i)
                    rowObjs[i] = rs.getObject(i + 1);

                assert rowPredicate.apply(rowObjs) : "Invalid row. [row=" + Arrays.toString(rowObjs) + ']';
            }
        }

        /** {@inheritDoc} */
        @Override public void check(int updateCount) {
            fail("Results set is expected. [updateCount=" + updateCount + ']');
        }
    }

    /**
     *
     */
    private static class Row {
        /** Row. */
        private final Object[] row;

        /**
         * @param row Data row.
         */
        private Row(Object[] row) {
            this.row = row;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Row row1 = (Row)o;

            return Arrays.equals(row, row1.row);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Arrays.hashCode(row);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return Arrays.toString(row);
        }
    }
}