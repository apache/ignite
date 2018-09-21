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
import org.apache.ignite.internal.processors.cache.CacheScanPartitionQueryFallbackSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheCrossCacheJoinRandomTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheObjectKeyIndexingSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePartitionedQueryMultiThreadedSelfTest;
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
import org.apache.ignite.internal.processors.query.h2.twostep.CacheQueryMemoryLeakTest;
import org.apache.ignite.internal.processors.query.h2.twostep.CreateTableWithDateKeySelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DisappearedCacheCauseRetryMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DisappearedCacheWasNotFoundMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.NonCollocatedRetryMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.RetryCauseMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.TableViewSubquerySelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 * Test suite for cache queries.
 */
public class IgniteCacheQuerySelfTestSuite2 extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new IgniteTestSuite("Ignite Cache Queries Test Suite 2");

        // Dynamic index create/drop tests.
        suite.addTestSuite(DynamicIndexPartitionedAtomicConcurrentSelfTest.class);
        suite.addTestSuite(DynamicIndexPartitionedTransactionalConcurrentSelfTest.class);
        suite.addTestSuite(DynamicIndexReplicatedAtomicConcurrentSelfTest.class);
        suite.addTestSuite(DynamicIndexReplicatedTransactionalConcurrentSelfTest.class);

        suite.addTestSuite(DynamicColumnsConcurrentAtomicPartitionedSelfTest.class);
        suite.addTestSuite(DynamicColumnsConcurrentTransactionalPartitionedSelfTest.class);
        suite.addTestSuite(DynamicColumnsConcurrentAtomicReplicatedSelfTest.class);
        suite.addTestSuite(DynamicColumnsConcurrentTransactionalReplicatedSelfTest.class);

        // Distributed joins.
        suite.addTestSuite(IgniteCacheQueryNodeRestartDistributedJoinSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest.class);

        // Other tests.
        suite.addTestSuite(IgniteCacheQueryMultiThreadedSelfTest.class);

        suite.addTestSuite(IgniteCacheQueryEvictsMultiThreadedSelfTest.class);

        suite.addTestSuite(ScanQueryOffheapExpiryPolicySelfTest.class);

        suite.addTestSuite(IgniteCacheCrossCacheJoinRandomTest.class);
        suite.addTestSuite(IgniteCacheClientQueryReplicatedNodeRestartSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryNodeFailTest.class);
        suite.addTestSuite(IgniteCacheQueryNodeRestartSelfTest.class);
        suite.addTestSuite(IgniteSqlQueryWithBaselineTest.class);
        suite.addTestSuite(IgniteChangingBaselineCacheQueryNodeRestartSelfTest.class);
        suite.addTestSuite(IgniteStableBaselineCacheQueryNodeRestartsSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryNodeRestartSelfTest2.class);
        suite.addTestSuite(IgniteCacheQueryNodeRestartTxSelfTest.class);
        suite.addTestSuite(IgniteCacheSqlQueryMultiThreadedSelfTest.class);
        suite.addTestSuite(IgniteCachePartitionedQueryMultiThreadedSelfTest.class);
        suite.addTestSuite(CacheScanPartitionQueryFallbackSelfTest.class);
        suite.addTestSuite(IgniteCacheDistributedQueryStopOnCancelOrTimeoutSelfTest.class);
        suite.addTestSuite(IgniteCacheObjectKeyIndexingSelfTest.class);

        suite.addTestSuite(IgniteCacheGroupsCompareQueryTest.class);
        suite.addTestSuite(IgniteCacheGroupsSqlSegmentedIndexSelfTest.class);
        suite.addTestSuite(IgniteCacheGroupsSqlSegmentedIndexMultiNodeSelfTest.class);
        suite.addTestSuite(IgniteCacheGroupsSqlDistributedJoinSelfTest.class);

        suite.addTestSuite(QueryJoinWithDifferentNodeFiltersTest.class);

        suite.addTestSuite(CacheQueryMemoryLeakTest.class);

        suite.addTestSuite(CreateTableWithDateKeySelfTest.class);

        suite.addTestSuite(NonCollocatedRetryMessageSelfTest.class);
        suite.addTestSuite(RetryCauseMessageSelfTest.class);
        suite.addTestSuite(DisappearedCacheCauseRetryMessageSelfTest.class);
        suite.addTestSuite(DisappearedCacheWasNotFoundMessageSelfTest.class);

        suite.addTestSuite(TableViewSubquerySelfTest.class);

        return suite;
    }
}
