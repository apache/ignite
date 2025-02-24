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
package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.util.typedef.F;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests index multi-range scans (with SEARCH/SARG operator or with dynamic parameters).
 */
@RunWith(Parameterized.class)
public class IndexMultiRangeScanIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** */
    @Parameterized.Parameter(1)
    public boolean dynamicParams;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "sqlTxMode={0},dynamicParams={1}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (boolean dynamicParams : new boolean[]{true, false}) {
            for (SqlTransactionMode sqlTxMode : SqlTransactionMode.values()) {
                params.add(new Object[]{sqlTxMode, dynamicParams});
            }
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected void init() throws Exception {
        super.init();

        sql("CREATE TABLE test (c1 INTEGER, c2 VARCHAR, c3 INTEGER) WITH " + atomicity());
        sql("CREATE INDEX c1c2c3 ON test(c1, c2, c3)");
        sql("CREATE TABLE test_desc (c1 INTEGER, c2 VARCHAR, c3 INTEGER) WITH " + atomicity());
        sql("CREATE INDEX c1c2c3_desc ON test_desc(c1 DESC, c2 DESC, c3 DESC)");
        sql("CREATE TABLE test_pk (c1 INTEGER, c2 VARCHAR, c3 INTEGER, PRIMARY KEY(c1, c2, c3)) WITH " + atomicity());

        for (String tbl : F.asList("test", "test_desc", "test_pk")) {
            sql("INSERT INTO " + tbl + "(c1, c2, c3) VALUES (0, null, 0)");

            for (int i = 0; i <= 5; i++) {
                for (int j = 1; j <= 5; j++)
                    sql("INSERT INTO " + tbl + "(c1, c2, c3) VALUES (?, ?, ?)", i == 0 ? null : i, Integer.toString(j), i * j);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        // Skip super method to keep caches after each test.
    }

    /** */
    @Test
    public void testIn() {
        for (String tbl : F.asList("test", "test_desc", "test_pk")) {
            log.info("Processing table: " + tbl);

            assertQuery("SELECT * FROM " + tbl + " WHERE c1 IN (%s, %s) AND c2 IN (%s, %s) AND c3 IN (%s, %s)",
                2, 3, "2", "3", 6, 9)
                .returns(2, "3", 6)
                .returns(3, "2", 6)
                .returns(3, "3", 9)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE (c1 = %s OR c1 IS NULL) AND c2 IN (%s, %s) AND c3 IN (%s, %s)",
                2, "2", "3", 0, 6)
                .returns(null, "2", 0)
                .returns(null, "3", 0)
                .returns(2, "3", 6)
                .check();
        }
    }

    /** */
    @Test
    public void testRange() {
        for (String tbl : F.asList("test", "test_desc", "test_pk")) {
            log.info("Processing table: " + tbl);

            assertQuery("SELECT * FROM " + tbl +
                    " WHERE ((c1 > %s AND c1 < %s) OR (c1 > %s AND c1 < %s)) AND c2 > %s AND c2 < %s",
                1, 3, 3, 5, "2", "5")
                .returns(2, "3", 6)
                .returns(2, "4", 8)
                .returns(4, "3", 12)
                .returns(4, "4", 16)
                .check();

            assertQuery("SELECT * FROM " + tbl +
                    " WHERE c1 IN (%s, %s) AND ((c2 >= %s AND c2 < %s) OR (c2 > %s AND c1 <= %s))",
                1, 2, "2", "3", "4", 5)
                .returns(1, "2", 2)
                .returns(1, "5", 5)
                .returns(2, "2", 4)
                .returns(2, "5", 10)
                .check();

            assertQuery("SELECT * FROM " + tbl +
                    " WHERE c1 IN (%s, %s) AND (c2 < %s OR (c2 >= %s AND c2 < %s) OR (c2 > %s AND c2 < %s) OR c2 > %s)",
                1, 2, "1", "2", "3", "2", "4", "5")
                .returns(1, "2", 2)
                .returns(1, "3", 3)
                .returns(2, "2", 4)
                .returns(2, "3", 6)
                .check();

            assertQuery("SELECT * FROM " + tbl +
                    " WHERE c1 = %s AND c2 > %s AND c3 IN (%s, %s)", 4, "3", 16, 20)
                .returns(4, "4", 16)
                .returns(4, "5", 20)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE (c1 < %s OR c1 >= %s) AND c2 = c1", 2, 4)
                .returns(1, "1", 1)
                .returns(4, "4", 16)
                .returns(5, "5", 25)
                .check();
        }
    }

    /** */
    @Test
    public void testNulls() {
        for (String tbl : F.asList("test", "test_desc", "test_pk")) {
            log.info("Processing table: " + tbl);

            assertQuery("SELECT * FROM " + tbl + " WHERE c1 IS NULL AND c2 <= %s", "1")
                .returns(null, "1", 0)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE (c1 IS NULL OR c1 = %s) AND c2 = %s AND c3 in (%s, %s)",
                3, "1", 0, 3)
                .returns(null, "1", 0)
                .returns(3, "1", 3)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE (c1 IS NULL OR c1 < %s) AND c2 = %s AND c3 in (%s, %s)",
                3, "1", 0, 1)
                .returns(null, "1", 0)
                .returns(1, "1", 1)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE (c1 IS NULL OR c1 > %s) AND c2 = %s AND c3 in (%s, %s)",
                3, "5", 0, 25)
                .returns(null, "5", 0)
                .returns(5, "5", 25)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE c1 IS NOT NULL AND c2 IS NULL")
                .returns(0, null, 0)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE c1 IN(NULL, %s) AND c2 = %s", 1, "1")
                .returns(1, "1", 1)
                .check();
        }
    }

    /** Tests not supported range index scan conditions. */
    @Test
    public void testNot() {
        assertQuery("SELECT * FROM test WHERE c1 <> %s AND c3 = %s", 1, 6)
            .matches(QueryChecker.containsTableScan("PUBLIC", "TEST")) // Can't use index scan.
            .returns(2, "3", 6)
            .returns(3, "2", 6)
            .check();

        assertQuery("SELECT * FROM test WHERE c1 NOT IN (%s, %s, %s) AND c2 NOT IN (%s, %s, %s)",
            1, 2, 5, 1, 2, 5)
            .matches(QueryChecker.containsTableScan("PUBLIC", "TEST")) // Can't use index scan.
            .returns(3, "3", 9)
            .returns(3, "4", 12)
            .returns(4, "3", 12)
            .returns(4, "4", 16)
            .check();
    }

    /** Test correct index ordering without additional sorting. */
    @Test
    public void testOrdering() {
        assertQuery("SELECT * FROM test WHERE c1 IN (%s, %s) AND c2 IN (%s, %s) ORDER BY c1, c2, c3",
            3, 2, "3", "2")
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort"))) // Don't require additional sorting.
            .ordered()
            .returns(2, "2", 4)
            .returns(2, "3", 6)
            .returns(3, "2", 6)
            .returns(3, "3", 9)
            .check();

        assertQuery("SELECT * FROM test WHERE c1 IN (%s, %s) AND c2 < %s ORDER BY c1, c2, c3", 2, 3, "3")
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort"))) // Don't require additional sorting.
            .ordered()
            .returns(2, "1", 2)
            .returns(2, "2", 4)
            .returns(3, "1", 3)
            .returns(3, "2", 6)
            .check();

        assertQuery("SELECT * FROM test WHERE c1 IN (%s, %s) AND c2 IN (%s, %s) AND c3 BETWEEN %s AND %s " +
            "ORDER BY c1, c2, c3", 2, 3, "2", "3", 5, 7)
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort"))) // Don't require additional sorting.
            .ordered()
            .returns(2, "3", 6)
            .returns(3, "2", 6)
            .check();

        // Check order for table with DESC ordering.
        assertQuery("SELECT * FROM test_desc " +
                "WHERE c1 IN (%s, %s) AND c2 IN (%s, %s) ORDER BY c1 DESC, c2 DESC, c3 DESC",
            3, 2, "3", "2")
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort"))) // Don't require additional sorting.
            .ordered()
            .returns(3, "3", 9)
            .returns(3, "2", 6)
            .returns(2, "3", 6)
            .returns(2, "2", 4)
            .check();

        assertQuery("SELECT * FROM test_desc " +
            "WHERE c1 IN (%s, %s) AND c2 < %s ORDER BY c1 DESC, c2 DESC, c3 DESC", 2, 3, "3")
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort"))) // Don't require additional sorting.
            .ordered()
            .returns(3, "2", 6)
            .returns(3, "1", 3)
            .returns(2, "2", 4)
            .returns(2, "1", 2)
            .check();

        assertQuery("SELECT * FROM test_desc WHERE c1 IN (%s, %s) AND c2 IN (%s, %s) AND c3 BETWEEN %s AND %s " +
            "ORDER BY c1 DESC, c2 DESC, c3 DESC", 2, 3, "2", "3", 5, 7)
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort"))) // Don't require additional sorting.
            .ordered()
            .returns(3, "2", 6)
            .returns(2, "3", 6)
            .check();
    }

    /** */
    @Test
    public void testRangeIntersection() {
        for (String tbl : F.asList("test", "test_desc", "test_pk")) {
            log.info("Processing table: " + tbl);

            assertQuery("SELECT * FROM " + tbl + " WHERE c1 IN (%s, %s, %s, %s) and c3 in (%s, %s, %s, %s)",
                3, 4, 4, 3, 12, 9, 16, 12)
                .returns(3, "3", 9)
                .returns(3, "4", 12)
                .returns(4, "3", 12)
                .returns(4, "4", 16)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE c1 IN (%s, %s) " +
                    "AND ((c2 > %s AND c2 < %s) OR (c2 > %s AND c2 < %s))",
                3, 4, "1", "4", "3", "5")
                .returns(3, "2", 6)
                .returns(3, "3", 9)
                .returns(3, "4", 12)
                .returns(4, "2", 8)
                .returns(4, "3", 12)
                .returns(4, "4", 16)
                .check();

            // Different combinations of LESS_THAN and LESS_THAN_OR_EQUAL.
            assertQuery("SELECT * FROM " + tbl + " WHERE c1 IN (%s, %s) AND (c2 < %s OR c2 < %s)",
                3, 4, "1", "2")
                .returns(3, "1", 3)
                .returns(4, "1", 4)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE c1 IN (%s, %s) AND (c2 <= %s OR c2 < %s)",
                3, 4, "1", "2")
                .returns(3, "1", 3)
                .returns(4, "1", 4)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE c1 IN (%s, %s) AND (c2 <= %s OR c2 <= %s)",
                3, 4, "1", "2")
                .returns(3, "1", 3)
                .returns(3, "2", 6)
                .returns(4, "1", 4)
                .returns(4, "2", 8)
                .check();

            // Different combinations of LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL.
            assertQuery("SELECT * FROM " + tbl + " WHERE c1 IN (%s, %s) " +
                    "AND ((c2 > %s AND c2 <= %s) OR (c2 > %s AND c2 <= %s) OR (c2 >= %s AND c2 < %s))",
                1, 2, "1", "2", "2", "3", "3", "4")
                .returns(1, "2", 2)
                .returns(1, "3", 3)
                .returns(2, "2", 4)
                .returns(2, "3", 6)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE c1 IN (%s, %s) AND c2 BETWEEN %s AND %s",
                3, 3, "2", "4")
                .returns(3, "2", 6)
                .returns(3, "3", 9)
                .returns(3, "4", 12)
                .check();
        }
    }

    /** */
    @Test
    public void testInvalidRange() {
        for (String tbl : F.asList("test", "test_desc", "test_pk")) {
            log.info("Processing table: " + tbl);

            assertQuery("SELECT * FROM " + tbl + " WHERE c1 BETWEEN %s AND %s", 4, 3)
                .resultSize(0)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE c1 = %s AND c2 > %s AND c2 < %s", 1, "2", "2")
                .resultSize(0)
                .check();

            assertQuery("SELECT * FROM " + tbl + " WHERE c1 = %s OR c1 BETWEEN %s AND %s", 1, 3, 2)
                .returns(1, "1", 1)
                .returns(1, "2", 2)
                .returns(1, "3", 3)
                .returns(1, "4", 4)
                .returns(1, "5", 5)
                .check();
        }
    }

    /** */
    private QueryChecker assertQuery(String pattern, Object... params) {
        Object[] args = new Object[params.length];

        if (dynamicParams) {
            Arrays.fill(args, "?");

            return assertQuery(String.format(pattern, args)).withParams(params);
        }
        else {
            for (int i = 0; i < params.length; i++)
                args[i] = params[i] instanceof String ? '\'' + params[i].toString() + '\'' : params[i].toString();

            return assertQuery(String.format(pattern, args));
        }
    }
}
