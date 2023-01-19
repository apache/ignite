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

import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessorTest;
import org.apache.ignite.internal.processors.query.calcite.CancelTest;
import org.apache.ignite.internal.processors.query.calcite.DateTimeTest;
import org.apache.ignite.internal.processors.query.calcite.SqlFieldsQueryUsageTest;
import org.apache.ignite.internal.processors.query.calcite.UnstableTopologyTest;
import org.apache.ignite.internal.processors.query.calcite.integration.AggregatesIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.CalciteBasicSecondaryIndexIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.CalciteErrorHandlilngIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.CorrelatesIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.DataTypesTest;
import org.apache.ignite.internal.processors.query.calcite.integration.DynamicParametersIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.ExpiredEntriesIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.FunctionsTest;
import org.apache.ignite.internal.processors.query.calcite.integration.HashSpoolIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IndexDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IndexRebuildIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IndexScanlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IndexSpoolIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IntervalTest;
import org.apache.ignite.internal.processors.query.calcite.integration.JoinIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.KeepBinaryIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.KillCommandDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.KillQueryCommandDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.LimitOffsetIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.LocalDateTimeSupportTest;
import org.apache.ignite.internal.processors.query.calcite.integration.MemoryQuotasIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.MetadataIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.QueryEngineConfigurationIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.QueryMetadataIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.RunningQueriesIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.SearchSargOnIndexIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.ServerStatisticsIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.SetOpIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.SortAggregateIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.StatisticsCommandDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.StdSqlOperatorsTest;
import org.apache.ignite.internal.processors.query.calcite.integration.SystemViewsIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.TableDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.TableDmlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.UserDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.UserDefinedFunctionsIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.jdbc.JdbcCrossEngineTest;
import org.apache.ignite.internal.processors.query.calcite.jdbc.JdbcQueryTest;
import org.apache.ignite.internal.processors.query.calcite.rules.JoinCommuteRulesTest;
import org.apache.ignite.internal.processors.query.calcite.rules.OrToUnionRuleTest;
import org.apache.ignite.internal.processors.query.calcite.rules.ProjectScanMergeRuleTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Calcite tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    OrToUnionRuleTest.class,
    ProjectScanMergeRuleTest.class,
    CalciteQueryProcessorTest.class,
    CalciteErrorHandlilngIntegrationTest.class,
    JdbcQueryTest.class,
    JdbcCrossEngineTest.class,
    CalciteBasicSecondaryIndexIntegrationTest.class,
    CancelTest.class,
    DateTimeTest.class,
    LimitOffsetIntegrationTest.class,
    SqlFieldsQueryUsageTest.class,
    AggregatesIntegrationTest.class,
    MetadataIntegrationTest.class,
    RunningQueriesIntegrationTest.class,
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
    SetOpIntegrationTest.class,
    UnstableTopologyTest.class,
    JoinCommuteRulesTest.class,
    ServerStatisticsIntegrationTest.class,
    JoinIntegrationTest.class,
    IntervalTest.class,
    UserDefinedFunctionsIntegrationTest.class,
    CorrelatesIntegrationTest.class,
    SystemViewsIntegrationTest.class,
    IndexRebuildIntegrationTest.class,
    QueryEngineConfigurationIntegrationTest.class,
    SearchSargOnIndexIntegrationTest.class,
    KeepBinaryIntegrationTest.class,
    QueryMetadataIntegrationTest.class,
    MemoryQuotasIntegrationTest.class,
    LocalDateTimeSupportTest.class,
    DynamicParametersIntegrationTest.class,
    ExpiredEntriesIntegrationTest.class,
})
public class IntegrationTestSuite {
}
