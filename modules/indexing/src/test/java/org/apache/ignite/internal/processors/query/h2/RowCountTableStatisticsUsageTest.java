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
package org.apache.ignite.internal.processors.query.h2;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Test cases to ensure that proper join order is chosen by H2 optimizer when row count statistics is collected.
 */
@RunWith(Parameterized.class)
public class RowCountTableStatisticsUsageTest extends TableStatisticsAbstractTest {
    /** */
    @Parameterized.Parameter(0)
    public CacheMode cacheMode;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "cacheMode={0}")
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
            { REPLICATED },
            { PARTITIONED },
        });
    }

    @Override protected void beforeTestsStarted() throws Exception {
        Ignite node = startGridsMultiThreaded(2);

        node.getOrCreateCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        runSql("DROP TABLE IF EXISTS big");
        runSql("DROP TABLE IF EXISTS med");
        runSql("DROP TABLE IF EXISTS small");

        runSql("CREATE TABLE big (a INT PRIMARY KEY, b INT, c INT) WITH \"TEMPLATE=" + cacheMode + "\"");
        runSql("CREATE TABLE med (a INT PRIMARY KEY, b INT, c INT) WITH \"TEMPLATE=" + cacheMode + "\"");
        runSql("CREATE TABLE small (a INT PRIMARY KEY, b INT, c INT) WITH \"TEMPLATE=" + cacheMode + "\"");

        runSql("CREATE INDEX big_b ON big(b)");
        runSql("CREATE INDEX med_b ON med(b)");
        runSql("CREATE INDEX small_b ON small(b)");

        runSql("CREATE INDEX big_c ON big(c)");
        runSql("CREATE INDEX med_c ON med(c)");
        runSql("CREATE INDEX small_c ON small(c)");

        for (int i = 0; i < BIG_SIZE; i++)
            runSql("INSERT INTO big(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");

        for (int i = 0; i < MED_SIZE; i++)
            runSql("INSERT INTO med(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");

        for (int i = 0; i < SMALL_SIZE; i++)
            runSql("INSERT INTO small(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");
    }

    /**
     *
     */
    @Test
    public void compareJoinsWithConditionsOnBothTables() {
        String sql = "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.c = t2.c " +
            "WHERE t1.b >= 0 AND t2.b >= 0";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "big");
    }

    /**
     *
     */
    @Test
    public void compareJoinsWithoutConditions() {
        String sql = "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.c = t2.c";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "big", "small");
    }

    /**
     *
     */
    @Test
    public void compareJoinsConditionSingleTable() {
        final String sql = "SELECT * FROM t1 JOIN t2 ON t1.c = t2.c WHERE t1.b >= 0";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "big", "small");
    }

    /**
     *
     */
    @Test
    public void compareJoinsThreeTablesNoConditions() {
        String sql = "SELECT * FROM t1 JOIN t2 ON t1.c = t2.c JOIN t3 ON t3.c = t2.c ";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "big", "med", "small");
        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "big", "med");
        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "med", "big");
        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "med", "big", "small");
    }

    /**
     *
     */
    @Test
    public void compareJoinsThreeTablesConditionsOnAllTables() {
        String sql = "SELECT * FROM t1 JOIN t2 ON t1.c = t2.c JOIN t3 ON t3.c = t2.c " +
            " WHERE t1.b >= 0 AND t2.b >= 0 AND t3.b >= 0";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "big", "med", "small");
        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "big", "med");
        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "med", "big");
        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "med", "big", "small");
    }

    /**
     * Checks if statistics is updated when table size is changed.
     */
    @Test
    public void checkUpdateStatisticsOnTableSizeChange() {
        // t2 size is bigger than t1
        String sql = "SELECT COUNT(*) FROM t2 JOIN t1 ON t1.c = t2.c " +
            "WHERE t1.b > " + 0 + " AND t2.b > " + 0;

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "big");

        // Make small table bigger than a big table
        for (int i = SMALL_SIZE; i < BIG_SIZE * 2; i++)
            runSql("INSERT INTO small(a, b, c) VALUES(" + i + "," + i + "," + i % 10 + ")");

        // t1 size is now bigger than t2
        sql = "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.c = t2.c " +
            "WHERE t1.b > " + 0 + " AND t2.b > " + 0;

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "small", "big");
    }

    /**
     *
     */
    @Test
    public void testStatisticsAfterRebalance() throws Exception {
        String sql = "SELECT COUNT(*) FROM t1 JOIN t2 ON t1.c = t2.c";

        checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "big", "small");

        startGrid(3);

        try {
            awaitPartitionMapExchange();

            grid(3).context()
                .cache()
                .context()
                .cacheContext(CU.cacheId("SQL_PUBLIC_BIG"))
                .preloader()
                .rebalanceFuture()
                .get(10_000);

            checkOptimalPlanChosenForDifferentJoinOrders(grid(1), sql, "big", "small");
            checkOptimalPlanChosenForDifferentJoinOrders(grid(0), sql, "big", "small");
        }
        finally {
            stopGrid(3);
        }
    }
}
