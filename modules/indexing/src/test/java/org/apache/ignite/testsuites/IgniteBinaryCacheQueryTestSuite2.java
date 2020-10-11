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

import org.apache.ignite.internal.metric.SqlStatisticsUserQueriesFastTest;
import org.apache.ignite.internal.metric.SqlStatisticsUserQueriesLongTest;
import org.apache.ignite.internal.processors.cache.CacheScanPartitionQueryFallbackSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheCrossCacheJoinRandomTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheObjectKeyIndexingSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePartitionedQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryEvictsMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.QueryJoinWithDifferentNodeFiltersTest;
import org.apache.ignite.internal.processors.cache.SqlCacheStartStopTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheClientQueryReplicatedNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedQueryDefaultTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedQueryStopOnCancelOrTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeFailTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest2;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartTxSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryReservationOnUnstableTopologyTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteSqlQueryWithBaselineTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxMultiNodeBasicTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentAtomicPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentAtomicReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentTransactionalPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentTransactionalReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicEnableIndexingBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicEnableIndexingConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexPartitionedAtomicConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexPartitionedTransactionalConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexReplicatedAtomicConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexReplicatedTransactionalConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQueryDefaultTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.query.ScanQueryOffheapExpiryPolicySelfTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteChangingBaselineCacheQueryNodeRestartSelfTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteStableBaselineCacheQueryNodeRestartsSelfTest;
import org.apache.ignite.internal.processors.query.CreateIndexOnInvalidDataTypeTest;
import org.apache.ignite.internal.processors.query.DisabledSqlFunctionsTest;
import org.apache.ignite.internal.processors.query.DmlBatchSizeDeadlockTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsCompareQueryTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsSqlDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsSqlSegmentedIndexMultiNodeSelfTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsSqlSegmentedIndexSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlCreateTableTemplateTest;
import org.apache.ignite.internal.processors.query.LocalQueryLazyTest;
import org.apache.ignite.internal.processors.query.LongRunningQueryTest;
import org.apache.ignite.internal.processors.query.SqlIndexConsistencyAfterInterruptAtomicCacheOperationTest;
import org.apache.ignite.internal.processors.query.SqlIndexConsistencyAfterInterruptTxCacheOperationTest;
import org.apache.ignite.internal.processors.query.SqlLocalQueryConnectionAndStatementTest;
import org.apache.ignite.internal.processors.query.SqlPartOfComplexPkLookupTest;
import org.apache.ignite.internal.processors.query.SqlQueriesTopologyMappingTest;
import org.apache.ignite.internal.processors.query.SqlTwoCachesInGroupWithSameEntryTest;
import org.apache.ignite.internal.processors.query.WrongQueryEntityFieldTypeTest;
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
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CreateIndexOnInvalidDataTypeTest.class,
    WrongQueryEntityFieldTypeTest.class,

    DisabledSqlFunctionsTest.class,

    SqlCacheStartStopTest.class,

    SqlIndexConsistencyAfterInterruptAtomicCacheOperationTest.class,
    SqlIndexConsistencyAfterInterruptTxCacheOperationTest.class,
    SqlTwoCachesInGroupWithSameEntryTest.class,

    // Dynamic index create/drop tests.
    DynamicIndexPartitionedAtomicConcurrentSelfTest.class,
    DynamicIndexPartitionedTransactionalConcurrentSelfTest.class,
    DynamicIndexReplicatedAtomicConcurrentSelfTest.class,
    DynamicIndexReplicatedTransactionalConcurrentSelfTest.class,

    DynamicColumnsConcurrentAtomicPartitionedSelfTest.class,
    DynamicColumnsConcurrentTransactionalPartitionedSelfTest.class,
    DynamicColumnsConcurrentAtomicReplicatedSelfTest.class,
    DynamicColumnsConcurrentTransactionalReplicatedSelfTest.class,

    DynamicEnableIndexingBasicSelfTest.class,
    DynamicEnableIndexingConcurrentSelfTest.class,

    // Distributed joins.
    IgniteCacheQueryNodeRestartDistributedJoinSelfTest.class,
    IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest.class,

    // Other tests.
    IgniteCacheQueryMultiThreadedSelfTest.class,

    IgniteCacheQueryEvictsMultiThreadedSelfTest.class,

    ScanQueryOffheapExpiryPolicySelfTest.class,

    IgniteCacheCrossCacheJoinRandomTest.class,
    IgniteCacheClientQueryReplicatedNodeRestartSelfTest.class,
    IgniteCacheQueryNodeFailTest.class,
    IgniteCacheQueryNodeRestartSelfTest.class,
    IgniteSqlQueryWithBaselineTest.class,
    IgniteChangingBaselineCacheQueryNodeRestartSelfTest.class,
    IgniteStableBaselineCacheQueryNodeRestartsSelfTest.class,
    IgniteCacheQueryNodeRestartSelfTest2.class,
    IgniteCacheQueryNodeRestartTxSelfTest.class,
    IgniteCacheSqlQueryMultiThreadedSelfTest.class,
    IgniteCachePartitionedQueryMultiThreadedSelfTest.class,
    CacheScanPartitionQueryFallbackSelfTest.class,
    IgniteCacheDistributedQueryDefaultTimeoutSelfTest.class,
    IgniteCacheDistributedQueryStopOnCancelOrTimeoutSelfTest.class,
    IgniteCacheObjectKeyIndexingSelfTest.class,

    IgniteCacheGroupsCompareQueryTest.class,
    IgniteCacheGroupsSqlSegmentedIndexSelfTest.class,
    IgniteCacheGroupsSqlSegmentedIndexMultiNodeSelfTest.class,
    IgniteCacheGroupsSqlDistributedJoinSelfTest.class,

    QueryJoinWithDifferentNodeFiltersTest.class,

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

    IgniteCacheLocalQueryDefaultTimeoutSelfTest.class,

    SqlQueriesTopologyMappingTest.class,

    IgniteCacheQueryReservationOnUnstableTopologyTest.class
})
public class IgniteBinaryCacheQueryTestSuite2 {
}
