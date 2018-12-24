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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
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
import org.apache.ignite.internal.processors.query.h2.twostep.CacheQueryMemoryLeakTest;
import org.apache.ignite.internal.processors.query.h2.twostep.CreateTableWithDateKeySelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DisappearedCacheCauseRetryMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DisappearedCacheWasNotFoundMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.NonCollocatedRetryMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.RetryCauseMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.TableViewSubquerySelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite for cache queries.
 */
@RunWith(AllTests.class)
public class IgniteBinaryCacheQueryTestSuite2 {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new IgniteTestSuite("Ignite Cache Queries Test Suite 2");

        // Dynamic index create/drop tests.
        suite.addTest(new JUnit4TestAdapter(DynamicIndexPartitionedAtomicConcurrentSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DynamicIndexPartitionedTransactionalConcurrentSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DynamicIndexReplicatedAtomicConcurrentSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DynamicIndexReplicatedTransactionalConcurrentSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(DynamicColumnsConcurrentAtomicPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DynamicColumnsConcurrentTransactionalPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DynamicColumnsConcurrentAtomicReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DynamicColumnsConcurrentTransactionalReplicatedSelfTest.class));

        // Distributed joins.
        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryNodeRestartDistributedJoinSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest.class));

        // Other tests.
        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryMultiThreadedSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryEvictsMultiThreadedSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(ScanQueryOffheapExpiryPolicySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheCrossCacheJoinRandomTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheClientQueryReplicatedNodeRestartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryNodeFailTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryNodeRestartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlQueryWithBaselineTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteChangingBaselineCacheQueryNodeRestartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteStableBaselineCacheQueryNodeRestartsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryNodeRestartSelfTest2.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryNodeRestartTxSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheSqlQueryMultiThreadedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedQueryMultiThreadedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheScanPartitionQueryFallbackSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDistributedQueryStopOnCancelOrTimeoutSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheObjectKeyIndexingSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheGroupsCompareQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheGroupsSqlSegmentedIndexSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheGroupsSqlSegmentedIndexMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheGroupsSqlDistributedJoinSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(QueryJoinWithDifferentNodeFiltersTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheQueryMemoryLeakTest.class));

        suite.addTest(new JUnit4TestAdapter(CreateTableWithDateKeySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(NonCollocatedRetryMessageSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(RetryCauseMessageSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DisappearedCacheCauseRetryMessageSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DisappearedCacheWasNotFoundMessageSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(TableViewSubquerySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueriesLoadTest1.class));

        return suite;
    }
}
