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

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.query.calcite.planner.AggregateDistinctPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.AggregatePlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.CorrelatedNestedLoopJoinPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.ExceptPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.HashAggregatePlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.HashIndexSpoolPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.JoinColocationPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.PlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.SortAggregatePlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.SortedIndexSpoolPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TableDmlPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TableFunctionTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TableSpoolPlannerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Calcite tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    PlannerTest.class,
    CorrelatedNestedLoopJoinPlannerTest.class,
    TableSpoolPlannerTest.class,
    SortedIndexSpoolPlannerTest.class,
    HashIndexSpoolPlannerTest.class,
    AggregatePlannerTest.class,
    AggregateDistinctPlannerTest.class,
    HashAggregatePlannerTest.class,
    SortAggregatePlannerTest.class,
    JoinColocationPlannerTest.class,
    ExceptPlannerTest.class,
    TableFunctionTest.class,
    TableDmlPlannerTest.class,
})
public class PlannerTestSuite {
}
