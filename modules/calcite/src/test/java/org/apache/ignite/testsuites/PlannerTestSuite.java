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

import org.apache.ignite.internal.processors.cache.DdlTransactionCalciteSelfTest;
import org.apache.ignite.internal.processors.cache.QueryEntityValueColumnAliasTest;
import org.apache.ignite.internal.processors.cache.SessionContextSqlFunctionTest;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessorPropertiesTest;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessorTest;
import org.apache.ignite.internal.processors.query.calcite.CancelTest;
import org.apache.ignite.internal.processors.query.calcite.IndexWithSameNameCalciteTest;
import org.apache.ignite.internal.processors.query.calcite.SqlFieldsQueryUsageTest;
import org.apache.ignite.internal.processors.query.calcite.integration.AggregatesIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.AuthorizationIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.CacheStoreTest;
import org.apache.ignite.internal.processors.query.calcite.integration.CacheWithInterceptorIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.CalciteBasicSecondaryIndexIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.CalciteErrorHandlilngIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.CalcitePlanningDumpTest;
import org.apache.ignite.internal.processors.query.calcite.integration.CorrelatesIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.DataTypesTest;
import org.apache.ignite.internal.processors.query.calcite.integration.DateTimeTest;
import org.apache.ignite.internal.processors.query.calcite.integration.DistributedJoinIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.DynamicParametersIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.ExpiredEntriesIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.FunctionsTest;
import org.apache.ignite.internal.processors.query.calcite.integration.HashSpoolIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IndexDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IndexMultiRangeScanIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IndexRebuildIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IndexScanMultiNodeIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IndexScanlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IndexSpoolIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IntervalTest;
import org.apache.ignite.internal.processors.query.calcite.integration.JoinIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.KeepBinaryIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.KeyClassChangeIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.KillCommandDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.KillQueryCommandDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.LimitOffsetIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.LocalDateTimeSupportTest;
import org.apache.ignite.internal.processors.query.calcite.integration.LocalQueryIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.MemoryQuotasIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.MetadataIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.MultiDcQueryMappingTest;
import org.apache.ignite.internal.processors.query.calcite.integration.OperatorsExtensionIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.PartitionPruneTest;
import org.apache.ignite.internal.processors.query.calcite.integration.PartitionsReservationIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.QueryBlockingTaskExecutorIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.QueryEngineConfigurationIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.QueryMetadataIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.QueryWithPartitionsIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.RunningQueriesIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.ScalarInIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.SelectByKeyFieldTest;
import org.apache.ignite.internal.processors.query.calcite.integration.ServerStatisticsIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.SetOpIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.SortAggregateIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.SqlDiagnosticIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.SqlPlanHistoryIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.StatisticsCommandDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.StdSqlOperatorsTest;
import org.apache.ignite.internal.processors.query.calcite.integration.SystemViewsIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.TableDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.TableDmlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.TimeoutIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.UnnestIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.UnstableTopologyIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.UserDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.UserDefinedFunctionsIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.UserDefinedFunctionsIntegrationTransactionalTest;
import org.apache.ignite.internal.processors.query.calcite.integration.UserDefinedTxAwareFunctionsIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.ViewsIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.tpch.TpchScale001Test;
import org.apache.ignite.internal.processors.query.calcite.integration.tpch.TpchScale010Test;
import org.apache.ignite.internal.processors.query.calcite.integration.tpch.TpchScale100Test;
import org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerUtilityTest;
import org.apache.ignite.internal.processors.query.calcite.planner.AggregateDistinctPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.AggregatePlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.CorrelatedNestedLoopJoinPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.CorrelatedSubqueryPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.DataTypesPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.HashAggregatePlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.HashIndexSpoolPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.HashJoinPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.IndexRebuildPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.IndexSearchBoundsPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.InlineIndexScanPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.JoinColocationPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.JoinCommutePlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.JoinWithUsingPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.LimitOffsetPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.MergeJoinPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.PlanExecutionTest;
import org.apache.ignite.internal.processors.query.calcite.planner.PlanSplitterTest;
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
import org.apache.ignite.internal.processors.query.calcite.rules.JoinCommuteRulesTest;
import org.apache.ignite.internal.processors.query.calcite.rules.JoinOrderOptimizationTest;
import org.apache.ignite.internal.processors.query.calcite.rules.OrToUnionRuleTest;
import org.apache.ignite.internal.processors.query.calcite.rules.ProjectScanMergeRuleTest;
import org.apache.ignite.internal.processors.query.calcite.thin.MultiLineQueryTest;
import org.apache.ignite.internal.processors.tx.TxThreadLockingTest;
import org.apache.ignite.internal.processors.tx.TxWithExceptionalInterceptorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Calcite tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    PlanExecutionTest.class,
    PlanSplitterTest.class,
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
    HashJoinPlannerTest.class,
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
    AbstractPlannerUtilityTest.class,

    HintsTestSuite.class,

    OrToUnionRuleTest.class,
    ProjectScanMergeRuleTest.class,
    CalciteQueryProcessorTest.class,
    CalciteQueryProcessorPropertiesTest.class,
    CalciteErrorHandlilngIntegrationTest.class,
    CalciteBasicSecondaryIndexIntegrationTest.class,
    CancelTest.class,
    DateTimeTest.class,
    LimitOffsetIntegrationTest.class,
    SqlFieldsQueryUsageTest.class,
    AggregatesIntegrationTest.class,
    MetadataIntegrationTest.class,
    RunningQueriesIntegrationTest.class,
    SqlDiagnosticIntegrationTest.class,
    SortAggregateIntegrationTest.class,
    TableDdlIntegrationTest.class,
    IndexDdlIntegrationTest.class,
    UserDdlIntegrationTest.class,
    KillCommandDdlIntegrationTest.class,
    KillQueryCommandDdlIntegrationTest.class,
    StatisticsCommandDdlIntegrationTest.class,
    FunctionsTest.class,
    StdSqlOperatorsTest.class,
    TableDmlIntegrationTest.class,
    DataTypesTest.class,
    IndexSpoolIntegrationTest.class,
    HashSpoolIntegrationTest.class,
    IndexScanlIntegrationTest.class,
    IndexScanMultiNodeIntegrationTest.class,
    SetOpIntegrationTest.class,
    UnstableTopologyIntegrationTest.class,
    PartitionsReservationIntegrationTest.class,
    JoinCommuteRulesTest.class,
    JoinOrderOptimizationTest.class,
    ServerStatisticsIntegrationTest.class,
    JoinIntegrationTest.class,
    IntervalTest.class,
    UserDefinedFunctionsIntegrationTest.class,
    UserDefinedFunctionsIntegrationTransactionalTest.class,
    CorrelatesIntegrationTest.class,
    SystemViewsIntegrationTest.class,
    IndexRebuildIntegrationTest.class,
    QueryEngineConfigurationIntegrationTest.class,
    IndexMultiRangeScanIntegrationTest.class,
    KeepBinaryIntegrationTest.class,
    LocalQueryIntegrationTest.class,
    QueryWithPartitionsIntegrationTest.class,
    QueryMetadataIntegrationTest.class,
    MemoryQuotasIntegrationTest.class,
    LocalDateTimeSupportTest.class,
    DynamicParametersIntegrationTest.class,
    ExpiredEntriesIntegrationTest.class,
    TimeoutIntegrationTest.class,
    PartitionPruneTest.class,
    DistributedJoinIntegrationTest.class,
    IndexWithSameNameCalciteTest.class,
    AuthorizationIntegrationTest.class,
    DdlTransactionCalciteSelfTest.class,
    MultiLineQueryTest.class,
    ViewsIntegrationTest.class,
    OperatorsExtensionIntegrationTest.class,
    SessionContextSqlFunctionTest.class,
    SqlPlanHistoryIntegrationTest.class,
    QueryBlockingTaskExecutorIntegrationTest.class,
    ScalarInIntegrationTest.class,
    TpchScale001Test.class,
    TpchScale010Test.class,
    TpchScale100Test.class,
    UnnestIntegrationTest.class,
    CalcitePlanningDumpTest.class,
    KeyClassChangeIntegrationTest.class,
    QueryEntityValueColumnAliasTest.class,
    CacheStoreTest.class,
    MultiDcQueryMappingTest.class,
    TxWithExceptionalInterceptorTest.class,
    UserDefinedTxAwareFunctionsIntegrationTest.class,
    CacheWithInterceptorIntegrationTest.class,
    TxThreadLockingTest.class,
    SelectByKeyFieldTest.class,
})
public class PlannerTestSuite {
}
