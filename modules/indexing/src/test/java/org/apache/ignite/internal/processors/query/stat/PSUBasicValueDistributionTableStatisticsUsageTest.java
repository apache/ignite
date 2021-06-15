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

package org.apache.ignite.internal.processors.query.stat;

import java.util.Arrays;
import java.util.Collection;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Planner statistics usage test: basic tests of value distribution statistics usage.
 */
@RunWith(Parameterized.class)
public class PSUBasicValueDistributionTableStatisticsUsageTest extends StatisticsAbstractTest {
    /** */
    @Parameterized.Parameter(0)
    public CacheMode cacheMode;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "cacheMode={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
            { REPLICATED },
            { PARTITIONED },
        });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite node = startGridsMultiThreaded(2);

        node.getOrCreateCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        sql("DROP TABLE IF EXISTS digital_distribution");

        sql("CREATE TABLE digital_distribution (ID INT PRIMARY KEY, col_a int, col_b int, col_c int, col_d int) " +
            "WITH \"TEMPLATE=" + cacheMode + "\"");

        sql("CREATE INDEX digital_distribution_col_a ON digital_distribution(col_a)");
        sql("CREATE INDEX digital_distribution_col_b ON digital_distribution(col_b)");
        sql("CREATE INDEX digital_distribution_col_c ON digital_distribution(col_c)");
        sql("CREATE INDEX digital_distribution_col_d ON digital_distribution(col_d)");

        for (int i = 0; i < 100; i++) {
            String sql = String.format("INSERT INTO digital_distribution(id, col_a, col_b, col_c, col_d)" +
                " VALUES(%d, %d, %d, 1, null)", i, i, i + 200);
            sql(sql);
        }
        for (int i = 100; i < 110; i++) {
            String sql = String.format("INSERT INTO digital_distribution(id, col_a, col_b, col_c) " +
                "VALUES(%d, null, %d, 1)", i, i + 200);
            sql(sql);
        }

        sql("DROP TABLE IF EXISTS empty_distribution");

        sql("CREATE TABLE empty_distribution (ID INT PRIMARY KEY, col_a int) " +
            "WITH \"TEMPLATE=" + cacheMode + "\"");

        sql("CREATE INDEX empty_distribution_col_a ON empty_distribution(col_a)");

        sql("DROP TABLE IF EXISTS empty_distribution_no_stat");

        sql("CREATE TABLE empty_distribution_no_stat (ID INT PRIMARY KEY, col_a int) " +
            "WITH \"TEMPLATE=" + cacheMode + "\"");

        sql("CREATE INDEX empty_distribution_no_stat_col_a ON empty_distribution_no_stat(col_a)");

        collectStatistics("digital_distribution", "empty_distribution");
    }

    /**
     * Select with two clauses:
     * 1) with "greater than" higher value in column (shouldn't find any rows)
     * 2) with "greater than" lower bound of column (should find almost all rows)
     * and check that first column index will be used.
     */
    @Test
    public void selectOverhightBorder() {
        String sql = "select count(*) from digital_distribution i1 where col_a > 200 and col_b > 200";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_A"}, sql,
            new String[1][]);
    }

    /**
     * Select with two clauses:
     * 1) with "less than" highest value in column (should find all rows)
     * 2) with "less than" lowest value in column (shouldn't find any rows)
     * and check that second column index will be used.
     */
    @Test
    public void selectOverflowBorder() {
        String sql = "select count(*) from digital_distribution i1 where col_a < 200 and col_b < 200";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_B"}, sql,
            new String[1][]);
    }

    /**
     * Select with two clauses for the same column
     * 1) with "greater than" lowest value in column (should find all rows)
     * 2) with "less than" highest value in column (should find all rows)
     * for column with only the same value in each row.
     * And one additional clause which filter out 10% of values.
     * Check that scan index will be chosen (that is how our cost function work - because of needs to read from scan
     * index flag).
     */
    @Test
    public void selectAroundTwoBorder() {
        String sql = "select count(*) from digital_distribution i1 where col_c > -50 and col_c < 50 and col_a < 90";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{}, sql, new String[1][]);
    }

    /**
     * Select with two clauses:
     * 1) with "is null" on column with single null value (should find one row)
     * 2) with "is null" on column without any null values (shouldn't find any rows)
     * and check that second column index will be used.
     */
    @Test
    public void selectNull() {
        String sql = "select count(*) from digital_distribution i1 where col_a is null and col_b is null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_B"}, sql,
            new String[1][]);
    }

    /**
     * Select with "higher than" clause from column with all the same values
     * and check that column index still will be used.
     */
    @Test
    public void selectHigherFromSingleValue() {
        String sql = "select count(*) from digital_distribution i1 where col_c > 1";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_C"}, sql,
            new String[1][]);
    }

    /**
     * Select with "lower than" clause from column with all the same values
     * and check that column index still will be used.
     */
    @Test
    public void selectLowerToSingleValue() {
        String sql = "select count(*) from digital_distribution i1 where col_c > 1";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_C"}, sql,
            new String[1][]);
    }

    /**
     * Select with "is null" clause from column with only null values
     * and check that column index still will be used.
     */
    @Test
    public void selectNullFromNull() {
        String sql = "select count(*) from digital_distribution i1 where col_d is null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_D"}, sql,
            new String[1][]);
    }

    /**
     * Select with "greater than" clause from column with only null values
     * and check that column index still will be used.
     */
    @Test
    public void selectGreaterFromNull() {
        String sql = "select count(*) from digital_distribution i1 where col_d > 0";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_D"}, sql,
            new String[1][]);
    }

    /**
     * Select with "less or equal" clause from column with only null values
     * and check that column index still will be used.
     */
    @Test
    public void selectLessOrEqualFromNull() {
        String sql = "select count(*) from digital_distribution i1 where col_d <= 1000";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_D"}, sql,
            new String[1][]);
    }

    /**
     * Select with "less or equal" clause from empty table without statistics
     * and check that column index will be used (correct behaviour without statistics).
     */
    @Test
    public void selectFromEmptyNoStatTable() {
        String sql = "select count(*) from empty_distribution_no_stat i1 where col_a <= 1000";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"EMPTY_DISTRIBUTION_NO_STAT_COL_A"}, sql,
            new String[1][]);
    }

    /**
     * Select with "is null" clause from empty table without statistics
     * and check that column index will be used (correct behaviour without statistics).
     */
    @Test
    public void selectNullFromEmptyNoStatTable() {
        String sql = "select count(*) from empty_distribution_no_stat i1 where col_a is null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"EMPTY_DISTRIBUTION_NO_STAT_COL_A"}, sql,
            new String[1][]);
    }

    /**
     * Select with "is not null" clause from empty table without statistics
     * and check that SCAN index will be used.
     */
    @Test
    public void selectNotNullFromEmptyNoStatTable() {
        String sql = "select count(*) from empty_distribution_no_stat i1 where col_a is not null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{}, sql, new String[1][]);
    }

    /**
     * Select with "less or equal" clause from empty table
     * and check that column index will be used.
     */
    @Test
    public void selectFromEmptyTable() {
        String sql = "select count(*) from empty_distribution i1 where col_a <= 1000";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"EMPTY_DISTRIBUTION_COL_A"}, sql,
            new String[1][]);
    }

    /**
     * Select with "is null" clause from empty table
     * and check that column index will be used.
     */
    @Test
    public void selectNullFromEmptyTable() {
        String sql = "select count(*) from empty_distribution i1 where col_a is null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"EMPTY_DISTRIBUTION_COL_A"}, sql,
            new String[1][]);
    }

    /**
     * Select with "is not null" clause from empty table
     * and check that scan index will be used.
     */
    @Test
    public void selectNotNullFromEmptyTable() {
        String sql = "select count(*) from empty_distribution i1 where col_a is not null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{}, sql, new String[1][]);
    }
}
