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

import org.apache.ignite.internal.processors.query.calcite.CalciteBasicSecondaryIndexIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessorTest;
import org.apache.ignite.internal.processors.query.calcite.CancelTest;
import org.apache.ignite.internal.processors.query.calcite.DataTypesTest;
import org.apache.ignite.internal.processors.query.calcite.DateTimeTest;
import org.apache.ignite.internal.processors.query.calcite.FunctionsTest;
import org.apache.ignite.internal.processors.query.calcite.LimitOffsetTest;
import org.apache.ignite.internal.processors.query.calcite.SqlFieldsQueryUsageTest;
import org.apache.ignite.internal.processors.query.calcite.UnstableTopologyTest;
import org.apache.ignite.internal.processors.query.calcite.integration.AggregatesIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.CalciteErrorHandlilngIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.IndexSpoolIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.MetadataIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.SetOpIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.SortAggregateIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.TableDdlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.integration.TableDmlIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.jdbc.JdbcQueryTest;
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
    CalciteBasicSecondaryIndexIntegrationTest.class,
    CancelTest.class,
    DateTimeTest.class,
    LimitOffsetTest.class,
    SqlFieldsQueryUsageTest.class,
    AggregatesIntegrationTest.class,
    MetadataIntegrationTest.class,
    SortAggregateIntegrationTest.class,
    TableDdlIntegrationTest.class,
    FunctionsTest.class,
    TableDmlIntegrationTest.class,
    DataTypesTest.class,
    IndexSpoolIntegrationTest.class,
    SetOpIntegrationTest.class,
    UnstableTopologyTest.class,
})
public class IntegrationTestSuite {
}
