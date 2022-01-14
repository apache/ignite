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

package org.apache.ignite.internal.runner.app.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Base class for complex SQL tests based on JDBC driver.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-16207")
public class ItJdbcComplexDmlDdlSelfTest extends AbstractJdbcSelfTest {
    /** Names of companies to use. */
    private static final List<String> COMPANIES = Arrays.asList("ASF", "GNU", "BSD");

    /** Cities to use. */
    private static final List<String> CITIES = Arrays.asList("St. Petersburg", "Boston", "Berkeley", "London");

    /**
     * Test CRD operations.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateSelectDrop() throws Exception {
        sql(new UpdateChecker(0),
                "CREATE TABLE person_t (ID int, NAME varchar, AGE int, COMPANY varchar, CITY varchar, "
                        + "primary key (ID, NAME, CITY))");

        sql(new UpdateChecker(0), "CREATE TABLE city (name varchar, population int, primary key (name))");

        sql(new UpdateChecker(3),
                "INSERT INTO city (name, population) values(?, ?), (?, ?), (?, ?)",
                "St. Petersburg", 6000000,
                "Boston", 2000000,
                "London", 8000000
        );

        sql(new ResultColumnChecker("id", "name", "age", "comp"),
                "SELECT id, name, age, company as comp FROM person_t where id < 50");

        for (int i = 0; i < 100; i++) {
            sql(new UpdateChecker(1),
                    "INSERT INTO person_t (id, name, age, company, city) values (?, ?, ?, ?, ?)",
                    i,
                    "Person " + i,
                    20 + (i % 10),
                    COMPANIES.get(i % COMPANIES.size()),
                    CITIES.get(i % CITIES.size())
            );
        }

        final int[] cnt = {0};

        sql(new ResultPredicateChecker((Object[] objs) -> {
            int id = ((Integer) objs[0]);

            if (id >= 50) {
                return false;
            }

            if (20 + (id % 10) != ((Integer) objs[2])) {
                return false;
            }

            if (!("Person " + id).equals(objs[1])) {
                return false;
            }

            ++cnt[0];

            return true;
        }), "SELECT id, name, age FROM person_t where id < 50");

        assertEquals(cnt[0], 50, "Invalid rows count");

        // Berkeley is not present in City table, although 25 people have it specified as their city.
        sql(new ResultChecker(new Object[][] {{75L}}),
                "SELECT COUNT(*) from person_t p inner join City c on p.city = c.name");

        sql(new UpdateChecker(34),
                "UPDATE person_t SET company = 'New Company', age = CASE WHEN MOD(id, 2) <> 0 THEN age + 5 ELSE "
                    + "age + 1 END WHERE company = 'ASF'");

        cnt[0] = 0;

        sql(new ResultPredicateChecker((Object[] objs) -> {
            int id = ((Integer) objs[0]);
            int age = ((Integer) objs[2]);

            if (id % 2 == 0) {
                if (age != 20 + (id % 10) + 1) {
                    return false;
                }
            } else {
                if (age != 20 + (id % 10) + 5) {
                    return false;
                }
            }

            ++cnt[0];

            return true;
        }), "SELECT * FROM person_t where company = 'New Company'");

        assertEquals(cnt[0], 34, "Invalid rows count");

        sql(new UpdateChecker(0), "DROP TABLE city");
        sql(new UpdateChecker(0), "DROP TABLE person_t");
    }

    /**
     * Run sql statement with arguments and check results.
     *
     * @param checker Query result's checker.
     * @param sql     SQL statement to execute.
     * @param args    Arguments.
     * @throws SQLException If failed.
     */
    protected void sql(SingleStatementChecker checker, String sql, Object... args) throws SQLException {
        Statement stmt = null;

        try {
            if (args.length > 0) {
                stmt = conn.prepareStatement(sql);

                PreparedStatement pstmt = (PreparedStatement) stmt;

                for (int i = 0; i < args.length; ++i) {
                    pstmt.setObject(i + 1, args[i]);
                }

                pstmt.execute();
            } else {
                stmt = conn.createStatement();

                stmt.execute(sql);
            }

            checkResults(stmt, checker);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    /**
     * Check query results with provided checker.
     *
     * @param stmt Statement.
     * @param checker Checker.
     * @throws SQLException If failed.
     */
    private void checkResults(Statement stmt, SingleStatementChecker checker) throws SQLException {
        ResultSet rs = stmt.getResultSet();

        if (rs != null) {
            checker.check(rs);
        } else {
            int updCnt = stmt.getUpdateCount();

            assert updCnt != -1 : "Invalid results. Result set is null and update count is -1";

            checker.check(updCnt);
        }
    }

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

    static class UpdateChecker implements SingleStatementChecker {
        /** Expected update count. */
        private final int expUpdCnt;

        /**
         * Constructor.
         *
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

    static class ResultChecker implements SingleStatementChecker {
        /** Expected update count. */
        private final Set<Row> expRs = new HashSet<>();

        /**
         * Constructor.
         *
         * @param expRs Expected result set.
         */
        ResultChecker(Object[][] expRs) {
            for (Object[] row : expRs) {
                this.expRs.add(new Row(row));
            }
        }

        /** {@inheritDoc} */
        @Override public void check(ResultSet rs) throws SQLException {
            int cols = rs.getMetaData().getColumnCount();

            while (rs.next()) {
                Object[] rowObjs = new Object[cols];

                for (int i = 0; i < cols; ++i) {
                    rowObjs[i] = rs.getObject(i + 1);
                }

                Row row = new Row(rowObjs);
                var rmv = expRs.remove(row);

                assert rmv : "Invalid row. [row=" + row + ", remainedRows="
                    + printRemainedExpectedResult() + ']';
            }

            assert expRs.isEmpty() : "Expected results has rows that aren't contained at the result set. [remainedRows="
                + printRemainedExpectedResult() + ']';
        }

        /** {@inheritDoc} */
        @Override public void check(int updateCount) {
            fail("Results set is expected. [updateCount=" + updateCount + ']');
        }

        private String printRemainedExpectedResult() {
            StringBuilder sb = new StringBuilder();

            for (Row r : expRs) {
                sb.append('\n').append(r.toString());
            }

            return sb.toString();
        }
    }

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

            assertEquals(cols, expColLabels.length, "Invalid columns count: [expected=" + expColLabels.length
                    + ", actual=" + cols + ']');

            for (int i = 0; i < cols; ++i) {
                assertTrue(expColLabels[i].equalsIgnoreCase(meta.getColumnLabel(i + 1)), expColLabels[i] + ":" + meta.getColumnName(i + 1));
            }

            super.check(rs);
        }
    }

    static class ResultPredicateChecker implements SingleStatementChecker {
        /** Row predicate. */
        private final Function<Object[], Boolean> rowPredicate;

        /**
         * Constructor.
         *
         * @param rowPredicate Row predicate to check result set.
         */
        ResultPredicateChecker(Function<Object[], Boolean> rowPredicate) {
            this.rowPredicate = rowPredicate;
        }

        /** {@inheritDoc} */
        @Override public void check(ResultSet rs) throws SQLException {
            int cols = rs.getMetaData().getColumnCount();

            while (rs.next()) {
                Object[] rowObjs = new Object[cols];

                for (int i = 0; i < cols; ++i) {
                    rowObjs[i] = rs.getObject(i + 1);
                }

                assert rowPredicate.apply(rowObjs) : "Invalid row. [row=" + Arrays.toString(rowObjs) + ']';
            }
        }

        /** {@inheritDoc} */
        @Override public void check(int updateCount) {
            fail("Results set is expected. [updateCount=" + updateCount + ']');
        }
    }

    private static class Row {
        /** Row. */
        private final Object[] row;

        /**
         * Conctructor.
         *
         * @param row Data row.
         */
        private Row(Object[] row) {
            this.row = row;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Row row1 = (Row) o;

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
