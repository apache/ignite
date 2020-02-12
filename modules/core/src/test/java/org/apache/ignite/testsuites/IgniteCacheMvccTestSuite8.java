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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorNearPartitionedAtomicCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorNearPartitionedAtomicCacheTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorPartitionedAtomicCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorPartitionedAtomicCacheTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorReplicatedAtomicCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorReplicatedAtomicCacheTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPartitionedMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPartitionedTckMetricsSelfTestImpl;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheMvccNearEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearAtomicMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRabalancingDelayedPartitionMapExchangeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheAtomicReplicatedMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.eviction.EvictionPolicyFailureHandlerTest;
import org.apache.ignite.internal.processors.cache.eviction.GridCacheEvictableEntryEqualsSelfTest;
import org.apache.ignite.internal.processors.cache.eviction.fifo.FifoEvictionPolicyFactorySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.fifo.FifoEvictionPolicySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.lru.LruEvictionPolicyFactorySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.lru.LruEvictionPolicySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.lru.LruNearEvictionPolicySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.lru.LruNearOnlyNearEvictionPolicySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionDataStreamerTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionMetricTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionPagesRecyclingAndReusingTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionReadThroughTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionTouchOrderTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.Random2LruNearEnabledPageEvictionMultinodeTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.Random2LruPageEvictionMultinodeTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.Random2LruPageEvictionWithRebalanceTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.RandomLruNearEnabledPageEvictionMultinodeTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.RandomLruPageEvictionMultinodeTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.RandomLruPageEvictionWithRebalanceTest;
import org.apache.ignite.internal.processors.cache.eviction.sorted.SortedEvictionPolicyFactorySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.sorted.SortedEvictionPolicySelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheAtomicLocalMetricsNoStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheAtomicLocalMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheAtomicLocalTckMetricsSelfTestImpl;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicMetricsNoReadThroughSelfTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgniteCacheMvccTestSuite8 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        HashSet<Class> ignoredTests = new HashSet<>(128);

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(LruNearEvictionPolicySelfTest.class);
        ignoredTests.add(LruNearOnlyNearEvictionPolicySelfTest.class);
        ignoredTests.add(RandomLruPageEvictionMultinodeTest.class);
        ignoredTests.add(RandomLruNearEnabledPageEvictionMultinodeTest.class);
        ignoredTests.add(PageEvictionDataStreamerTest.class);
        ignoredTests.add(Random2LruPageEvictionMultinodeTest.class);
        ignoredTests.add(Random2LruNearEnabledPageEvictionMultinodeTest.class);
        ignoredTests.add(RandomLruPageEvictionWithRebalanceTest.class);
        ignoredTests.add(Random2LruPageEvictionWithRebalanceTest.class);
        ignoredTests.add(PageEvictionTouchOrderTest.class);
        ignoredTests.add(PageEvictionReadThroughTest.class);
        ignoredTests.add(PageEvictionMetricTest.class);
        ignoredTests.add(PageEvictionPagesRecyclingAndReusingTest.class);

        // Irrelevant Tx tests.
        ignoredTests.add(GridCacheEvictableEntryEqualsSelfTest.class);

        // Atomic cache tests.
        ignoredTests.add(GridCacheLocalAtomicMetricsNoReadThroughSelfTest.class);
        ignoredTests.add(GridCacheNearAtomicMetricsSelfTest.class);
        ignoredTests.add(GridCacheAtomicLocalMetricsSelfTest.class);
        ignoredTests.add(GridCacheAtomicLocalMetricsNoStoreSelfTest.class);
        ignoredTests.add(GridCacheAtomicReplicatedMetricsSelfTest.class);
        ignoredTests.add(GridCacheAtomicPartitionedMetricsSelfTest.class);
        ignoredTests.add(GridCacheAtomicPartitionedTckMetricsSelfTestImpl.class);
        ignoredTests.add(GridCacheAtomicLocalTckMetricsSelfTestImpl.class);
        ignoredTests.add(IgniteTopologyValidatorPartitionedAtomicCacheTest.class);
        ignoredTests.add(IgniteTopologyValidatorNearPartitionedAtomicCacheTest.class);
        ignoredTests.add(IgniteTopologyValidatorReplicatedAtomicCacheTest.class);
        ignoredTests.add(IgniteTopologyValidatorNearPartitionedAtomicCacheGroupsTest.class);
        ignoredTests.add(IgniteTopologyValidatorPartitionedAtomicCacheGroupsTest.class);
        ignoredTests.add(IgniteTopologyValidatorReplicatedAtomicCacheGroupsTest.class);

        // Other non-tx tests.
        ignoredTests.add(FifoEvictionPolicySelfTest.class);
        ignoredTests.add(SortedEvictionPolicySelfTest.class);
        ignoredTests.add(LruEvictionPolicySelfTest.class);
        ignoredTests.add(FifoEvictionPolicyFactorySelfTest.class);
        ignoredTests.add(SortedEvictionPolicyFactorySelfTest.class);
        ignoredTests.add(LruEvictionPolicyFactorySelfTest.class);
        ignoredTests.add(EvictionPolicyFailureHandlerTest.class);
        ignoredTests.add(GridCacheAtomicNearEvictionSelfTest.class);
        ignoredTests.add(GridCacheRabalancingDelayedPartitionMapExchangeSelfTest.class);

        // Skip classes which Mvcc implementations are added in this method below.
        // TODO IGNITE-10175: refactor these tests (use assume) to support both mvcc and non-mvcc modes after moving to JUnit4/5.
        ignoredTests.add(GridCacheNearEvictionSelfTest.class); // See GridCacheMvccNearEvictionSelfTest

        List<Class<?>> suite = new ArrayList<>(IgniteCacheTestSuite8.suite(ignoredTests));

        // Add Mvcc clones.
        suite.add(GridCacheMvccNearEvictionSelfTest.class);

        return suite;
    }
}
