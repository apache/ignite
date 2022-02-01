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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.junit.Test;

/**
 * Planner statistics usage test: partial statistics collection (by set of columns) tests.
 */
public class PSUStatisticPartialGatheringTest extends StatisticsAbstractTest {
    /** */
    private static final String SQL = "select * from TBL_SELECT i1 where lo_select = %d and med_select = %d";

    /** */
    private static final String[][] NO_HINTS = new String[1][];

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite node = startGridsMultiThreaded(1);

        node.getOrCreateCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        sql("DROP TABLE IF EXISTS TBL_SELECT");

        sql("CREATE TABLE TBL_SELECT (ID INT PRIMARY KEY, lo_select int, med_select int, hi_select int)");

        sql("CREATE INDEX TBL_SELECT_LO_IDX ON TBL_SELECT(lo_select)");
        sql("CREATE INDEX TBL_SELECT_MED_IDX ON TBL_SELECT(med_select)");
        sql("CREATE INDEX TBL_SELECT_HI_IDX ON TBL_SELECT(hi_select)");

        for (int i = 0; i < 1000; i++)
            sql(String.format("insert into tbl_select(id, lo_select, med_select, hi_select) values(%d, %d, %d, %d)",
                i, i % 10, i % 100, i % 1000));

        collectStatistics("tbl_select");
    }

    /**
     * Test that partial statistics collection work properly:
     *
     * Prepare:
     * 1) create table with three columns with lo, med and hi selectivity
     * 2) collect statistics
     *
     * Test:
     * 1) select with equals clauses by lo and med selectivity columns and test that med_idx used
     * 2) update data in lo selectivity so it will have hi selectivity
     * 3) collect statistics by hi selectivity column
     * 4) test that select still use med idx because of outdated statistics
     * 5) collect statistics by lo selectivity column
     * 6) test that select start to use lo_idx because now it has better selectivity than med one
     */
    @Test
    public void compareSelectWithIntConditions() throws IgniteCheckedException {
        System.out.println("+++ " + sql("select * from sys.statistics_local_data"));
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"TBL_SELECT_MED_IDX"},
            String.format(SQL, 5, 5), NO_HINTS);

        sql("UPDATE TBL_SELECT SET lo_select = hi_select");

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"TBL_SELECT_MED_IDX"},
            String.format(SQL, 6, 6), NO_HINTS);

        // All columns set up before also will be updated
        updateStatistics(new StatisticsTarget(SCHEMA, "TBL_SELECT", "LO_SELECT"));

        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"TBL_SELECT_LO_IDX"},
            String.format(SQL, 8, 8), NO_HINTS);
    }
}
