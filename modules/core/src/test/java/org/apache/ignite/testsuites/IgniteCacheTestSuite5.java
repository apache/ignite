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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.GridCacheAffinityBackupsSelfTest;
import org.apache.ignite.IgniteCacheAffinitySelfTest;
import org.apache.ignite.cache.affinity.AffinityClientNodeSelfTest;
import org.apache.ignite.cache.affinity.AffinityDistributionLoggingTest;
import org.apache.ignite.cache.affinity.AffinityHistoryCleanupTest;
import org.apache.ignite.cache.affinity.local.LocalAffinityFunctionTest;
import org.apache.ignite.internal.GridCachePartitionExchangeManagerHistSizeTest;
import org.apache.ignite.internal.GridCachePartitionExchangeManagerWarningsTest;
import org.apache.ignite.internal.processors.cache.CacheKeepBinaryTransactionTest;
import org.apache.ignite.internal.processors.cache.CacheNearReaderUpdateTest;
import org.apache.ignite.internal.processors.cache.CacheRebalancingSelfTest;
import org.apache.ignite.internal.processors.cache.CacheSerializableTransactionsTest;
import org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTest;
import org.apache.ignite.internal.processors.cache.ClusterStatePartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.ClusterStateReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.ConcurrentCacheStartTest;
import org.apache.ignite.internal.processors.cache.EntryVersionConsistencyReadThroughTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePutStackOverflowSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheReadThroughEvictionsVariationsSuite;
import org.apache.ignite.internal.processors.cache.IgniteCacheStoreCollectionTest;
import org.apache.ignite.internal.processors.cache.PartitionsExchangeOnDiscoveryHistoryOverflowTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLateAffinityAssignmentNodeJoinValidationTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLateAffinityAssignmentTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheGroupsPartitionLossPolicySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCachePartitionLossPolicySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheTxIteratorSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.NotMappedPartitionInTxTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.IgniteCacheAtomicProtocolTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.CacheManualRebalancingTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheSyncRebalanceModeSelfTest;
import org.apache.ignite.internal.processors.cache.store.IgniteCacheWriteBehindNoUpdateSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite5 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        GridTestUtils.addTestIfNeeded(suite, CacheSerializableTransactionsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheNearReaderUpdateTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheStoreCollectionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheWriteBehindNoUpdateSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePutStackOverflowSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheKeepBinaryTransactionTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheLateAffinityAssignmentTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheLateAffinityAssignmentNodeJoinValidationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, EntryVersionConsistencyReadThroughTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheSyncRebalanceModeSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxIteratorSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, ClusterStatePartitionedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterStateReplicatedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClusterReadOnlyModeTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePartitionLossPolicySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheGroupsPartitionLossPolicySelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheRebalancingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheManualRebalancingTest.class, ignoredTests);

        // Affinity tests.
        GridTestUtils.addTestIfNeeded(suite, GridCacheAffinityBackupsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAffinitySelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AffinityClientNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, LocalAffinityFunctionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AffinityHistoryCleanupTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, AffinityDistributionLoggingTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicProtocolTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, PartitionsExchangeOnDiscoveryHistoryOverflowTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionExchangeManagerHistSizeTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionExchangeManagerWarningsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, NotMappedPartitionInTxTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, ConcurrentCacheStartTest.class, ignoredTests);

        suite.add(IgniteCacheReadThroughEvictionsVariationsSuite.class);

        //GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicPreloadSelfTest.class, ignoredTests);
        //GridTestUtils.addTestIfNeeded(suite, IgniteCacheContainsKeyColocatedAtomicSelfTest.class, ignoredTests);
        //GridTestUtils.addTestIfNeeded(suite, IgniteCacheContainsKeyNearAtomicSelfTest.class, ignoredTests);

        return suite;
    }
}
