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

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.metric.SqlStatisticsUserQueriesFastTest;
import org.apache.ignite.internal.metric.SqlStatisticsUserQueriesLongTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryReservationOnUnstableTopologyTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxMultiNodeBasicTest;
import org.apache.ignite.internal.processors.cache.query.ScanQueryTransactionIsolationTest;
import org.apache.ignite.internal.processors.cache.query.ScanQueryTransactionsUnsupportedModesTest;
import org.apache.ignite.internal.processors.query.DmlBatchSizeDeadlockTest;
import org.apache.ignite.internal.processors.query.IgniteSqlCreateTableTemplateTest;
import org.apache.ignite.internal.processors.query.LocalQueryLazyTest;
import org.apache.ignite.internal.processors.query.LongRunningQueryTest;
import org.apache.ignite.internal.processors.query.SqlLocalQueryConnectionAndStatementTest;
import org.apache.ignite.internal.processors.query.SqlPartOfComplexPkLookupTest;
import org.apache.ignite.internal.processors.query.SqlQueriesTopologyMappingTest;
import org.apache.ignite.internal.processors.query.h2.CacheQueryEntityWithDateTimeApiFieldsTest;
import org.apache.ignite.internal.processors.query.h2.DmlStatementsProcessorTest;
import org.apache.ignite.internal.processors.query.h2.twostep.CacheQueryMemoryLeakTest;
import org.apache.ignite.internal.processors.query.h2.twostep.CreateTableWithDateKeySelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DisappearedCacheCauseRetryMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DisappearedCacheWasNotFoundMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.NonCollocatedRetryMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.NoneOrSinglePartitionsQueryOptimizationsTest;
import org.apache.ignite.internal.processors.query.h2.twostep.RetryCauseMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.TableViewSubquerySelfTest;
import org.apache.ignite.sqltests.SqlByteArrayTest;
import org.apache.ignite.sqltests.SqlDataTypesCoverageTests;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CacheQueryMemoryLeakTest.class,

    CreateTableWithDateKeySelfTest.class,

    CacheQueryEntityWithDateTimeApiFieldsTest.class,

    DmlStatementsProcessorTest.class,

    NonCollocatedRetryMessageSelfTest.class,
    RetryCauseMessageSelfTest.class,
    DisappearedCacheCauseRetryMessageSelfTest.class,
    DisappearedCacheWasNotFoundMessageSelfTest.class,

    TableViewSubquerySelfTest.class,

    SqlLocalQueryConnectionAndStatementTest.class,

    NoneOrSinglePartitionsQueryOptimizationsTest.class,

    IgniteSqlCreateTableTemplateTest.class,

    LocalQueryLazyTest.class,

    LongRunningQueryTest.class,

    SqlStatisticsUserQueriesFastTest.class,
    SqlStatisticsUserQueriesLongTest.class,

    DmlBatchSizeDeadlockTest.class,

    GridCachePartitionedTxMultiNodeSelfTest.class,
    GridCacheReplicatedTxMultiNodeBasicTest.class,

    SqlPartOfComplexPkLookupTest.class,

    SqlDataTypesCoverageTests.class,
    SqlByteArrayTest.class,
    SqlPartOfComplexPkLookupTest.class,

    SqlQueriesTopologyMappingTest.class,

    IgniteCacheQueryReservationOnUnstableTopologyTest.class,

    ScanQueryTransactionsUnsupportedModesTest.class,
    ScanQueryTransactionIsolationTest.class
})
public class IgniteBinaryCacheQueryTestSuite4 {
    /** Setup lazy mode default. */
    @BeforeClass
    public static void setupLazy() {
        GridTestUtils.setFieldValue(SqlFieldsQuery.class, "DFLT_LAZY", false);
    }
}
