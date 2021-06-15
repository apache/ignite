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
import org.junit.Ignore;
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
public class PSUValueDistributionTableStatisticsUsageTest extends StatisticsAbstractTest {
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
        sql("DROP TABLE IF EXISTS sized");

        sql("CREATE TABLE sized (ID INT PRIMARY KEY, small VARCHAR, small_nulls VARCHAR," +
            " big VARCHAR, big_nulls VARCHAR) WITH \"TEMPLATE=" + cacheMode + "\"");

        sql("CREATE INDEX sized_small ON sized(small)");
        sql("CREATE INDEX sized_small_nulls ON sized(small_nulls)");
        sql("CREATE INDEX sized_big ON sized(big)");
        sql("CREATE INDEX sized_big_nulls ON sized(big_nulls)");

        String bigVal = "someBigLongValueWithTheSameTextAtFirst";
        String smallNulls, bigNulls;
        int valAdd;
        for (int i = 0; i < BIG_SIZE; i++) {
            if ((i & 1) == 0) {
                smallNulls = "null";
                bigNulls = null;
                valAdd = 0;
            } else {
                smallNulls = String.format("'small%d'", i);
                bigNulls = String.format("'%s%d'", bigVal, i);
                valAdd = 1;
            }
            String sql = String.format("INSERT INTO sized(id, small, small_nulls, big, big_nulls)" +
                " VALUES(%d,'small%d', %s, '%s%d', %s)", i, i + valAdd, smallNulls, bigVal, i + valAdd, bigNulls);
            sql(sql);
        }
        sql("INSERT INTO sized(id, small, big) VALUES(" + BIG_SIZE + ", null, null)");

        collectStatistics("sized");
    }

    /**
     * Select with only one clause with "is null" condition and check that right index will be selected.
     */
    @Test
    public void selectNullCond() {
        String sql = "select count(*) from sized i1 where small is null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL"}, sql, new String[1][]);
    }

    // TODO create Ignite mirror ticket and set it here
    @Ignore("https://ggsystems.atlassian.net/browse/GG-31183")
    @Test
    public void selectNotNullCond() {
        String sql = "select count(*) from sized i1 where small is not null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL"}, sql, new String[1][]);
    }

    /**
     * Select with "is null" clause from two columns and check that index by the column with less nulls chosen.
     */
    @Test
    public void selectWithNullsDistributionCond() {
        String sql = "select * from sized i1 where small is null and small_nulls is null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL"}, sql, new String[1][]);
    }

    /**
     * Check that nulls percent take into account by planner:
     * 1) select with only one clause and test that appropriate index will be chosen.
     * 2) select with two "equal to constant" clause and test that column with nulls will be chosen.
     */
    @Test
    public void selectWithValueNullsDistributionCond() {
        String sql = "select * from sized i1 where small = '1'";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL"}, sql, new String[1][]);

        String sql2 = "select * from sized i1 where small = '1' and small_nulls = '1'";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL_NULLS"}, sql2, new String[1][]);
    }

    @Ignore("https://issues.apache.org/jira/browse/IGNITE-14813")
    @Test
    public void selectWithValueSizeCond() {
        String sql = "select * from sized i1 where big = '1' and small = '1'";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL"}, sql, new String[1][]);

        String sql2 = "select * from sized i1 where big_nulls = '1' and small_nulls = '1'";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"SIZED_SMALL_NULLS"}, sql2, new String[1][]);
    }
}
