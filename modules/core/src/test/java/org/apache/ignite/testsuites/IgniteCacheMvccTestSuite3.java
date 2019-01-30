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
import org.apache.ignite.internal.processors.cache.CacheInterceptorPartitionCounterLocalSanityTest;
import org.apache.ignite.internal.processors.cache.CacheInterceptorPartitionCounterRandomOperationsTest;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicEntryProcessorDeploymentSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheEntryVersionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorAtomicRebalanceTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorAtomicReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorAtomicWithStoreReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorAtomicWithStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorLocalAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorLocalAtomicWithStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheValueBytesPreloadingSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheVersionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheVersionTopologyChangeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryAtomicEntryProcessorDeploymentSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAsyncOperationsTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMixedModeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheClientOnlySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteTxReentryColocatedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheValueConsistencyAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheValueConsistencyAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOnlySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteTxReentryNearSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMvccTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMvccTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMvccTxTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreSelfTest;
import org.apache.ignite.internal.processors.cache.store.IgnteCacheClientWriteBehindStoreAtomicTest;
import org.apache.ignite.internal.processors.cache.store.IgnteCacheClientWriteBehindStoreNonCoalescingTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheMvccTestSuite3 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        HashSet<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(GridCacheEntryVersionSelfTest.class);
        ignoredTests.add(GridCacheVersionTopologyChangeTest.class);
        ignoredTests.add(CacheAsyncOperationsTest.class);
        ignoredTests.add(CacheInterceptorPartitionCounterLocalSanityTest.class);
        ignoredTests.add(CacheInterceptorPartitionCounterRandomOperationsTest.class);
        ignoredTests.add(IgniteCacheGroupsTest.class);

        // Atomic caches
        ignoredTests.add(GridCacheValueConsistencyAtomicSelfTest.class);
        ignoredTests.add(GridCacheValueConsistencyAtomicNearEnabledSelfTest.class);
        ignoredTests.add(GridCacheReplicatedAtomicGetAndTransformStoreSelfTest.class);
        ignoredTests.add(GridCacheAtomicEntryProcessorDeploymentSelfTest.class);
        ignoredTests.add(GridCacheValueBytesPreloadingSelfTest.class);
        ignoredTests.add(GridCacheBinaryAtomicEntryProcessorDeploymentSelfTest.class);

        ignoredTests.add(GridCacheClientOnlySelfTest.CasePartitionedAtomic.class);
        ignoredTests.add(GridCacheClientOnlySelfTest.CaseReplicatedAtomic.class);
        ignoredTests.add(GridCacheNearOnlySelfTest.CasePartitionedAtomic.class);
        ignoredTests.add(GridCacheNearOnlySelfTest.CaseReplicatedAtomic.class);

        ignoredTests.add(IgnteCacheClientWriteBehindStoreAtomicTest.class);
        ignoredTests.add(IgnteCacheClientWriteBehindStoreNonCoalescingTest.class);

        ignoredTests.add(GridCacheInterceptorLocalAtomicSelfTest.class);
        ignoredTests.add(GridCacheInterceptorLocalAtomicWithStoreSelfTest.class);
        ignoredTests.add(GridCacheInterceptorAtomicSelfTest.class);
        ignoredTests.add(GridCacheInterceptorAtomicNearEnabledSelfTest.class);
        ignoredTests.add(GridCacheInterceptorAtomicWithStoreSelfTest.class);
        ignoredTests.add(GridCacheInterceptorAtomicReplicatedSelfTest.class);
        ignoredTests.add(GridCacheInterceptorAtomicWithStoreReplicatedSelfTest.class);
        ignoredTests.add(GridCacheInterceptorAtomicRebalanceTest.class);

        // Irrelevant tx tests
        ignoredTests.add(IgniteTxReentryNearSelfTest.class);
        ignoredTests.add(IgniteTxReentryColocatedSelfTest.class);

        // Other non-tx tests
        ignoredTests.add(GridCacheWriteBehindStoreSelfTest.class);
        ignoredTests.add(GridCacheWriteBehindStoreMultithreadedSelfTest.class);

        ignoredTests.add(GridCacheVersionSelfTest.class);
        ignoredTests.add(GridCacheMixedModeSelfTest.class);

        // Skip classes which Mvcc implementations are added in this method below.
        // TODO IGNITE-10175: refactor these tests (use assume) to support both mvcc and non-mvcc modes after moving to JUnit4/5.
        ignoredTests.add(GridCacheReplicatedTxSingleThreadedSelfTest.class); // See GridCacheReplicatedMvccTxSingleThreadedSelfTest
        ignoredTests.add(GridCacheReplicatedTxMultiThreadedSelfTest.class); // See GridCacheReplicatedMvccTxMultiThreadedSelfTest
        ignoredTests.add(GridCacheReplicatedTxTimeoutSelfTest.class); // See GridCacheReplicatedMvccTxTimeoutSelfTest

        List<Class<?>> suite = new ArrayList<>(IgniteBinaryObjectsCacheTestSuite3.suite(ignoredTests));

        // Add Mvcc clones.
        suite.add(GridCacheReplicatedMvccTxSingleThreadedSelfTest.class);
        suite.add(GridCacheReplicatedMvccTxMultiThreadedSelfTest.class);
        suite.add(GridCacheReplicatedMvccTxTimeoutSelfTest.class);

        return suite;
    }
}
