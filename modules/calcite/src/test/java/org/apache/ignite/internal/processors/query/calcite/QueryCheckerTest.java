/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite;

import org.hamcrest.Matcher;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsResultRowCount;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.matchesOnce;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Query checker tests. */
public class QueryCheckerTest {
    /** */
    @Test
    public void testMatchesOnce() {
        String planMatchesOnce = "PLAN=IgniteExchange(distribution=[single])\n  " +
            "IgniteProject(NAME=[$2])\n    " +
            "IgniteTableScan(table=[[PUBLIC, DEVELOPER]], projects=[[$t0]], requiredColunms=[{2}])\n  " +
            "IgniteTableScan(table=[[PUBLIC, DEVELOPER]], projects=[[$t1]], requiredColunms=[{2, 3}])";

        Matcher<String> matcherTbl = matchesOnce("IgniteTableScan");
        Matcher<String> matcherPrj = matchesOnce("IgniteProject");

        assertFalse(matcherTbl.matches(planMatchesOnce));
        assertTrue(matcherPrj.matches(planMatchesOnce));
    }

    /**
     * Check that result row query matcher match result rows and doesn't match scan row count.
     */
    @Test
    public void testContainsResultRow() {
        String plan = "    IgniteMapHashAggregate(group=[{}], COUNT(NAME)=[COUNT($0)]): rowcount = 1.0, " +
            "cumulative cost = IgniteCost [rowCount=2000.0, cpu=2000.0, memory=5.0, io=0.0, network=0.0], id = 43\n" +
            "      IgniteTableScan(table=[[PUBLIC, PERSON]], requiredColumns=[{2}]): rowcount = 1000.0, " +
            "cumulative cost = IgniteCost [rowCount=1000.0, cpu=1000.0, memory=0.0, io=0.0, network=0.0], id = 34";

        Matcher<String> containsScan = containsResultRowCount(2000);
        Matcher<String> containsResult = containsResultRowCount(1);

        assertFalse(containsScan.matches(plan));
        assertTrue(containsResult.matches(plan));
    }
}
