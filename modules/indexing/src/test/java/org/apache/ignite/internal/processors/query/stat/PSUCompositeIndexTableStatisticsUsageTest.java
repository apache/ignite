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
 * Planner statistics usage test: ensure that proper index is chosen by H2 optimizer when value distribution statistics
 * is collected.
 */
@RunWith(Parameterized.class)
public class PSUCompositeIndexTableStatisticsUsageTest extends StatisticsAbstractTest {
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
        sql("DROP TABLE IF EXISTS ci_table");

        sql("CREATE TABLE ci_table (ID INT, col_a int, col_b int, col_c int, col_d int, " +
            "PRIMARY key (ID, col_a, col_b, col_c)) WITH \"TEMPLATE=" + cacheMode + "\"");

        sql("CREATE INDEX ci_table_abc ON ci_table(col_a, col_b, col_c)");

        for (int i = 0; i < BIG_SIZE; i++) {
            String sql = String.format("INSERT INTO ci_table(id, col_a, col_b, col_c) VALUES(%d, %d, %d, %d)", i, i,
                i * 2, i * 10);
            sql(sql);
        }

        collectStatistics("ci_table");
    }

    /**
     * 1) Check that "is null" equal clause with column.null() == 0 lead to complex index selection.
     * 2) Add index for "less than" column and update statistics.
     * 3) Check that complex index still used.
     *
     * Select contain conditions with:
     * 1) two clauses with "is null" condition for the first indexed columns
     * 2) one clause with "less than" condition for the third indexed columns
     * And check that right index will be selected.
     */
    @Test
    public void selectAllColumns() {
        String sql = "select count(*) from ci_table i1 where col_a is null and col_b is null and col_c < 2";

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"CI_TABLE_ABC"}, sql, new String[1][]);

        sql("CREATE INDEX ci_table_c ON ci_table(col_c)");
        updateStatistics("ci_table");

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"CI_TABLE_ABC"}, sql, new String[1][]);

        sql("DROP INDEX IF EXISTS ci_table_c");
        updateStatistics("ci_table");
    }

    /**
     * 1) Check that "is null" equal clause with column.null() == 0 lead to complex index selection.
     * 2) Add index for "less than" column and update statistics.
     * 3) Check that complex index still used.
     *
     * Select contain conditions with:
     * 1) two clauses with "is not null" condition for the first indexed columns
     * 2) one clause with "equal" condition for the third indexed columns
     * And check that scan index will be selected.
     */
    @Test
    public void selectAllColumns2() {
        String sql = "select count(*) from ci_table i1 where col_a is not null and col_b is not null and col_c = 2";

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{}, sql, new String[1][]);

        sql("CREATE INDEX ci_table_c ON ci_table(col_c)");
        updateStatistics("ci_table");

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"CI_TABLE_C"}, sql, new String[1][]);

        sql("DROP INDEX IF EXISTS ci_table_c");
        updateStatistics("ci_table");
    }

    /**
     * Select with only one clause with "is null" condition and PK clause and check that right index will be selected.
     */
    @Test
    public void selectIdAndAllColumns() {
        String sql = "select count(*) from ci_table i1 where id = 1 and col_a is null and col_b is null and col_c < 2";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{}, sql, new String[1][]);
    }
}
