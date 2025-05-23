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
import org.apache.ignite.internal.processors.query.calcite.planner.CorrelatedSubqueryPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.DataTypesPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.HashAggregatePlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.HashIndexSpoolPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.IndexRebuildPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.IndexSearchBoundsPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.InlineIndexScanPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.JoinColocationPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.JoinCommutePlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.JoinWithUsingPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.LimitOffsetPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.MergeJoinPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.PlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.PlannerTimeoutTest;
import org.apache.ignite.internal.processors.query.calcite.planner.ProjectFilterScanMergePlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.RexSimplificationPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.SerializationPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.SetOpPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.SortAggregatePlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.SortedIndexSpoolPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.StatisticsPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TableDmlPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TableFunctionPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TableSpoolPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.UncollectPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.UnionPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.UserDefinedViewsPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.hints.HintsTestSuite;
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
    SetOpPlannerTest.class,
    TableFunctionPlannerTest.class,
    TableDmlPlannerTest.class,
    UnionPlannerTest.class,
    DataTypesPlannerTest.class,
    JoinCommutePlannerTest.class,
    LimitOffsetPlannerTest.class,
    MergeJoinPlannerTest.class,
    StatisticsPlannerTest.class,
    CorrelatedSubqueryPlannerTest.class,
    JoinWithUsingPlannerTest.class,
    ProjectFilterScanMergePlannerTest.class,
    IndexRebuildPlannerTest.class,
    PlannerTimeoutTest.class,
    IndexSearchBoundsPlannerTest.class,
    InlineIndexScanPlannerTest.class,
    UserDefinedViewsPlannerTest.class,
    RexSimplificationPlannerTest.class,
    SerializationPlannerTest.class,
    UncollectPlannerTest.class,

    HintsTestSuite.class,
})
public class PlannerTestSuite {
}
