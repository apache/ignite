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
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.query.h2.twostep.CacheQueryMemoryLeakTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DisappearedCacheCauseRetryMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DisappearedCacheWasNotFoundMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.NonCollocatedRetryMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.RetryCauseMessageSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.TableViewSubquerySelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 * Test suite for cache queries.
 */
public class IgniteBinaryCacheQueryTestSuite2 extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
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

        suite.addTest(new JUnit4TestAdapter(NonCollocatedRetryMessageSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(RetryCauseMessageSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DisappearedCacheCauseRetryMessageSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DisappearedCacheWasNotFoundMessageSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(TableViewSubquerySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueriesLoadTest1.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlCreateTableTemplateTest.class));

        return suite;
    }
}
