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
import org.apache.ignite.internal.processors.cache.GridCacheDeploymentOffHeapSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheDeploymentSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheEntryVersionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOrderedPreloadingSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReferenceCleanupSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReloadSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedSynchronousCommitTest;
import org.apache.ignite.internal.processors.cache.GridCacheValueBytesPreloadingSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheValueConsistencyTransactionalNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheValueConsistencyTransactionalSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheVersionSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInterceptorSelfTestSuite;
import org.apache.ignite.internal.processors.cache.distributed.CacheAsyncOperationsTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMixedModeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxGetAfterStopTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDaemonNodePartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedOnlyP2PDisabledByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedOnlyP2PEnabledByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedTransformWriteThroughBatchUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteTxReentryColocatedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheValueConsistencyAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheValueConsistencyAtomicPrimaryWriteOrderNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheValueConsistencyAtomicPrimaryWriteOrderSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheValueConsistencyAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearPartitionedP2PDisabledByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearPartitionedP2PEnabledByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePutArrayValueSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteTxReentryNearSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheDaemonNodeReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedBasicApiTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedBasicOpSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedBasicStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedEvictionEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedInvalidateSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMultiNodeLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedP2PDisabledByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedP2PEnabledByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedPreloadEventsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedUnswapAdvancedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheSyncReplicatedPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridReplicatedTxPreloadTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.preloader.GridCacheReplicatedPreloadLifecycleSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.preloader.GridCacheReplicatedPreloadOffHeapSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.preloader.GridCacheReplicatedPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.preloader.GridCacheReplicatedPreloadStartStopEventsSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheDaemonNodeLocalSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalByteArrayValuesSelfTest;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite3 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("IgniteCache Test Suite part 3");

        // Value consistency tests.
        suite.addTestSuite(GridCacheValueConsistencyAtomicSelfTest.class);
        suite.addTestSuite(GridCacheValueConsistencyAtomicPrimaryWriteOrderSelfTest.class);
        suite.addTestSuite(GridCacheValueConsistencyAtomicNearEnabledSelfTest.class);
        suite.addTestSuite(GridCacheValueConsistencyAtomicPrimaryWriteOrderNearEnabledSelfTest.class);
        suite.addTestSuite(GridCacheValueConsistencyTransactionalSelfTest.class);
        suite.addTestSuite(GridCacheValueConsistencyTransactionalNearEnabledSelfTest.class);
        suite.addTestSuite(GridCacheValueBytesPreloadingSelfTest.class);

        // Replicated cache.
        suite.addTestSuite(GridCacheReplicatedBasicApiTest.class);
        suite.addTestSuite(GridCacheReplicatedBasicOpSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedBasicStoreSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedGetAndTransformStoreSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedAtomicGetAndTransformStoreSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedEventSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedSynchronousCommitTest.class);

        suite.addTestSuite(GridCacheReplicatedInvalidateSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedLockSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedMultiNodeLockSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedMultiNodeSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedNodeFailureSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedTxSingleThreadedSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedTxTimeoutSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedPreloadSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedPreloadOffHeapSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedPreloadLifecycleSelfTest.class);
        suite.addTestSuite(GridCacheSyncReplicatedPreloadSelfTest.class);

        suite.addTestSuite(GridCacheDeploymentSelfTest.class);
        suite.addTestSuite(GridCacheDeploymentOffHeapSelfTest.class);

        suite.addTestSuite(GridCachePutArrayValueSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedUnswapAdvancedSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedEvictionEventSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedTxMultiThreadedSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedPreloadEventsSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedPreloadStartStopEventsSelfTest.class);
        suite.addTestSuite(GridReplicatedTxPreloadTest.class);

        suite.addTestSuite(IgniteTxReentryNearSelfTest.class);
        suite.addTestSuite(IgniteTxReentryColocatedSelfTest.class);

        suite.addTestSuite(GridCacheOrderedPreloadingSelfTest.class);

        // Test for byte array value special case.
        suite.addTestSuite(GridCacheLocalByteArrayValuesSelfTest.class);
        suite.addTestSuite(GridCacheNearPartitionedP2PEnabledByteArrayValuesSelfTest.class);
        suite.addTestSuite(GridCacheNearPartitionedP2PDisabledByteArrayValuesSelfTest.class);
        suite.addTestSuite(GridCachePartitionedOnlyP2PEnabledByteArrayValuesSelfTest.class);
        suite.addTestSuite(GridCachePartitionedOnlyP2PDisabledByteArrayValuesSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedP2PEnabledByteArrayValuesSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedP2PDisabledByteArrayValuesSelfTest.class);

        // Near-only cache.
        suite.addTest(IgniteCacheNearOnlySelfTestSuite.suite());

        // Test cache with daemon nodes.
        suite.addTestSuite(GridCacheDaemonNodeLocalSelfTest.class);
        suite.addTestSuite(GridCacheDaemonNodePartitionedSelfTest.class);
        suite.addTestSuite(GridCacheDaemonNodeReplicatedSelfTest.class);

        // Write-behind.
        suite.addTest(IgniteCacheWriteBehindTestSuite.suite());

        // Transform.
        suite.addTestSuite(GridCachePartitionedTransformWriteThroughBatchUpdateSelfTest.class);

        suite.addTestSuite(GridCacheEntryVersionSelfTest.class);
        suite.addTestSuite(GridCacheVersionSelfTest.class);

        // Memory leak tests.
        suite.addTestSuite(GridCacheReferenceCleanupSelfTest.class);
        suite.addTestSuite(GridCacheReloadSelfTest.class);

        suite.addTestSuite(GridCacheMixedModeSelfTest.class);

        // Cache metrics.
        suite.addTest(IgniteCacheMetricsSelfTestSuite.suite());

        // Topology validator.
        suite.addTest(IgniteTopologyValidatorTestSuit.suite());

        // Eviction.
        suite.addTest(IgniteCacheEvictionSelfTestSuite.suite());

        // Iterators.
        suite.addTest(IgniteCacheIteratorsSelfTestSuite.suite());

        // Cache interceptor tests.
        suite.addTest(IgniteCacheInterceptorSelfTestSuite.suite());

        suite.addTestSuite(IgniteTxGetAfterStopTest.class);

        suite.addTestSuite(CacheAsyncOperationsTest.class);

        return suite;
    }
}