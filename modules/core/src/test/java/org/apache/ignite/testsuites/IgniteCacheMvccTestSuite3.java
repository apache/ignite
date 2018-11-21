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

import java.util.HashSet;
import junit.framework.TestSuite;
import org.apache.ignite.IgniteSystemProperties;
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
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryAtomicEntryProcessorDeploymentSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAsyncOperationsTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMixedModeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheClientOnlySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheValueConsistencyAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheValueConsistencyAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOnlySelfTest;
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

/**
 * Test suite.
 */
public class IgniteCacheMvccTestSuite3 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     */
    public static TestSuite suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        HashSet<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(GridCacheEntryVersionSelfTest.class);
        ignoredTests.add(GridCacheVersionTopologyChangeTest.class);
        ignoredTests.add(CacheAsyncOperationsTest.class);
        ignoredTests.add(GridCacheBinaryAtomicEntryProcessorDeploymentSelfTest.class);

        // Atomic caches
        ignoredTests.add(GridCacheValueConsistencyAtomicSelfTest.class);
        ignoredTests.add(GridCacheValueConsistencyAtomicNearEnabledSelfTest.class);
        ignoredTests.add(GridCacheReplicatedAtomicGetAndTransformStoreSelfTest.class);
        ignoredTests.add(GridCacheAtomicEntryProcessorDeploymentSelfTest.class);
        ignoredTests.add(GridCacheValueBytesPreloadingSelfTest.class);

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

        TestSuite suite = new TestSuite("IgniteCache Mvcc Test Suite part 3");

        suite.addTest(IgniteBinaryObjectsCacheTestSuite3.suite(ignoredTests));

        // Add Mvcc clones.
        suite.addTestSuite(GridCacheReplicatedMvccTxSingleThreadedSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedMvccTxMultiThreadedSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedMvccTxTimeoutSelfTest.class);

        return suite;
    }
}
