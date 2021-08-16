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

import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.matchesOnce;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Query checker tests. */
public class QueryCheckerTest {
    /** */
    private static final String PLAN = "IgniteSingleHashAggregate(group=[{}], COUNT(NAME)=[COUNT($0)]): rowcount = 1.0, " +
        "cumulative cost = IgniteCost [rowCount=9.0, cpu=9.0, memory=5.0, io=0.0, network=12.0], id = 110\n" +
        "  IgniteExchange(distribution=[single]): rowcount = 3.0, cumulative cost = IgniteCost " +
        "[rowCount=6.0, cpu=6.0, memory=0.0, io=0.0, network=12.0], id = 109\n" +
        "    IgniteIndexScan(table=[[PUBLIC, PERSON]], index=[PERSON_NAME], requiredColumns=[{2}]): rowcount = 3.0, " +
        "cumulative cost = IgniteCost [rowCount=3.0, cpu=3.0, memory=0.0, io=0.0, network=0.0], id = 29";

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
     * Check that the wrong cost will not be found.
     */
    @Test
    public void testCostMatchesWrongCostNoMatches() {
        TestCost wrongCost = new TestCost(4., null, null, null, null);
        Matcher<String> wrongCostMatcher = QueryChecker.containsCost(wrongCost);

        assertFalse(wrongCostMatcher.matches(PLAN));
    }

    /**
     * Check that the any cost will be found.
     */
    @Test
    public void testCostMatchesAnyCostMatches() {
        TestCost anyCost = new TestCost(null, null, null, null, null);
        Matcher<String> anyCostMatcher = QueryChecker.containsCost(anyCost);

        assertTrue(anyCostMatcher.matches(PLAN));
    }

    /**
     * Check that the cost from the first line of plan will be found.
     */
    @Test
    public void testCostMatchesFirstLineMatches() {
        TestCost firstLineCost = new TestCost(null, 9., null, null, 12.);
        Matcher<String> firstLineCostMatcher = QueryChecker.containsCost(firstLineCost);

        assertTrue(firstLineCostMatcher.matches(PLAN));
    }

    /**
     * Check that the cost from the middle line of plan will be found.
     */
    @Test
    public void testCostMatchesMiddleLineMatches() {
        TestCost middleLineCost = new TestCost(6., 6., null, 0., 12.);
        Matcher<String> middleLineCostMatcher = QueryChecker.containsCost(middleLineCost);

        assertTrue(middleLineCostMatcher.matches(PLAN));
    }

    /**
     * Check that the cost from the last line of plan will be found.
     */
    @Test
    public void testCostMatchesLastLineMatches() {
        TestCost lastLineCost = new TestCost(3., 3., null, 0., null);
        Matcher<String> lastLineCostMatcher = QueryChecker.containsCost(lastLineCost);

        assertTrue(lastLineCostMatcher.matches(PLAN));
    }
}
