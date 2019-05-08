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

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueriesLoadTest1;
import org.apache.ignite.internal.processors.query.IgniteSqlCreateTableTemplateTest;

/**
 * Cache query suite with binary marshaller.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Dynamic index create/drop tests.
//    DynamicIndexPartitionedAtomicConcurrentSelfTest.class,
    DynamicIndexPartitionedTransactionalConcurrentSelfTest.class,
    DynamicIndexReplicatedAtomicConcurrentSelfTest.class,
//    DynamicIndexReplicatedTransactionalConcurrentSelfTest.class,
//
//    DynamicColumnsConcurrentAtomicPartitionedSelfTest.class,
//    DynamicColumnsConcurrentTransactionalPartitionedSelfTest.class,
//    DynamicColumnsConcurrentAtomicReplicatedSelfTest.class,
//    DynamicColumnsConcurrentTransactionalReplicatedSelfTest.class,
//
//    // Distributed joins.
//    IgniteCacheQueryNodeRestartDistributedJoinSelfTest.class,
//    IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest.class,
//
//    // Other tests.
//    IgniteCacheQueryMultiThreadedSelfTest.class,
//
//    IgniteCacheQueryEvictsMultiThreadedSelfTest.class,
//
//    ScanQueryOffheapExpiryPolicySelfTest.class,
//
//    IgniteCacheCrossCacheJoinRandomTest.class,
    IgniteCacheClientQueryReplicatedNodeRestartSelfTest.class,
//    IgniteCacheQueryNodeFailTest.class,
//    IgniteCacheQueryNodeRestartSelfTest.class,
//    IgniteSqlQueryWithBaselineTest.class,
//    IgniteChangingBaselineCacheQueryNodeRestartSelfTest.class,
//    IgniteStableBaselineCacheQueryNodeRestartsSelfTest.class,
//    IgniteCacheQueryNodeRestartSelfTest2.class,
//    IgniteCacheQueryNodeRestartTxSelfTest.class,
//    IgniteCacheSqlQueryMultiThreadedSelfTest.class,
//    IgniteCachePartitionedQueryMultiThreadedSelfTest.class,
//    CacheScanPartitionQueryFallbackSelfTest.class,
//    IgniteCacheDistributedQueryStopOnCancelOrTimeoutSelfTest.class,
//    IgniteCacheObjectKeyIndexingSelfTest.class,
//
//    IgniteCacheGroupsCompareQueryTest.class,
//    IgniteCacheGroupsSqlSegmentedIndexSelfTest.class,
//    IgniteCacheGroupsSqlSegmentedIndexMultiNodeSelfTest.class,
//    IgniteCacheGroupsSqlDistributedJoinSelfTest.class,
//
//    QueryJoinWithDifferentNodeFiltersTest.class,
//
//    CacheQueryMemoryLeakTest.class,
//
//    CreateTableWithDateKeySelfTest.class,
//
//    CacheQueryEntityWithDateTimeApiFieldsTest.class,
//
//    DmlStatementsProcessorTest.class,
//
//    NonCollocatedRetryMessageSelfTest.class,
//    RetryCauseMessageSelfTest.class,
//    DisappearedCacheCauseRetryMessageSelfTest.class,
//    DisappearedCacheWasNotFoundMessageSelfTest.class,
//
//    TableViewSubquerySelfTest.class,
//
//    IgniteCacheQueriesLoadTest1.class,
//
//    SqlLocalQueryConnectionAndStatementTest.class,
//
//    NoneOrSinglePartitionsQueryOptimizationsTest.class,
//
//    IgniteSqlCreateTableTemplateTest.class,
//
//    LocalQueryLazyTest.class,
//
//    LongRunningQueryTest.class,
//    DmlBatchSizeDeadlockTest.class
})
public class IgniteBinaryCacheQueryTestSuite2 {
}
