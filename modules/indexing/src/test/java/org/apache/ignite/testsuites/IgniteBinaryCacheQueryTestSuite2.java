/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.cache.CacheScanPartitionQueryFallbackSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheCrossCacheJoinRandomTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheObjectKeyIndexingSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePartitionedQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueriesLoadTest1;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryEvictsMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.QueryJoinWithDifferentNodeFiltersTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheClientQueryReplicatedNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedQueryStopOnCancelOrTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeFailTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest2;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartTxSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteSqlQueryWithBaselineTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentAtomicPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentAtomicReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentTransactionalPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentTransactionalReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexPartitionedAtomicConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexPartitionedTransactionalConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexReplicatedAtomicConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexReplicatedTransactionalConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.query.ScanQueryOffheapExpiryPolicySelfTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteChangingBaselineCacheQueryNodeRestartSelfTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteStableBaselineCacheQueryNodeRestartsSelfTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsCompareQueryTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsSqlDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsSqlSegmentedIndexMultiNodeSelfTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsSqlSegmentedIndexSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlCreateTableTemplateTest;
import org.apache.ignite.internal.processors.query.SqlLocalQueryConnectionAndStatementTest;
import org.apache.ignite.internal.processors.query.h2.CacheQueryEntityWithDateTimeApiFieldsTest;
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
    // Dynamic index create/drop tests.
    DynamicIndexPartitionedAtomicConcurrentSelfTest.class,
    DynamicIndexPartitionedTransactionalConcurrentSelfTest.class,
    DynamicIndexReplicatedAtomicConcurrentSelfTest.class,
    DynamicIndexReplicatedTransactionalConcurrentSelfTest.class,

    DynamicColumnsConcurrentAtomicPartitionedSelfTest.class,
    DynamicColumnsConcurrentTransactionalPartitionedSelfTest.class,
    DynamicColumnsConcurrentAtomicReplicatedSelfTest.class,
    DynamicColumnsConcurrentTransactionalReplicatedSelfTest.class,

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

    NonCollocatedRetryMessageSelfTest.class,
    RetryCauseMessageSelfTest.class,
    DisappearedCacheCauseRetryMessageSelfTest.class,
    DisappearedCacheWasNotFoundMessageSelfTest.class,

    TableViewSubquerySelfTest.class,

    IgniteCacheQueriesLoadTest1.class,

    SqlLocalQueryConnectionAndStatementTest.class,

    NoneOrSinglePartitionsQueryOptimizationsTest.class,

    IgniteSqlCreateTableTemplateTest.class,
})
public class IgniteBinaryCacheQueryTestSuite2 {
}
