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
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.cache.CacheStartupInDeploymentModesTest;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicEntryProcessorDeploymentSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConditionalDeploymentSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheEntryVersionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReferenceCleanupSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReloadSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedSynchronousCommitTest;
import org.apache.ignite.internal.processors.cache.GridCacheTransactionalEntryProcessorDeploymentSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheValueBytesPreloadingSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheValueConsistencyTransactionalNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheValueConsistencyTransactionalSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheVersionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheVersionTopologyChangeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInterceptorSelfTestSuite;
import org.apache.ignite.internal.processors.cache.IgniteCacheScanPredicateDeploymentSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAsyncOperationsTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLockChangingTopologyTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMixedModeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxGetAfterStopTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxRemoveTimeoutObjectsNearTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxRemoveTimeoutObjectsTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDaemonNodePartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedOnlyP2PDisabledByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedOnlyP2PEnabledByteArrayValuesSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedTransformWriteThroughBatchUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteTxReentryColocatedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheValueConsistencyAtomicNearEnabledSelfTest;
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
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedEventDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedEvictionEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedGetAndTransformStoreSelfTest;
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
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheSyncReplicatedPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.preloader.GridCacheReplicatedPreloadLifecycleSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.preloader.GridCacheReplicatedPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.preloader.GridCacheReplicatedPreloadStartStopEventsSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheDaemonNodeLocalSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalByteArrayValuesSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite3 {
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

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheGroupsTest.class, ignoredTests);

        // Value consistency tests.
        GridTestUtils.addTestIfNeeded(suite, GridCacheValueConsistencyAtomicSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheValueConsistencyAtomicNearEnabledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheValueConsistencyTransactionalSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheValueConsistencyTransactionalNearEnabledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheValueBytesPreloadingSelfTest.class, ignoredTests);

        // Replicated cache.
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedBasicApiTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedBasicOpSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedBasicStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedGetAndTransformStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedAtomicGetAndTransformStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedEventSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedEventDisabledSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedSynchronousCommitTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedLockSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedMultiNodeLockSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedNodeFailureSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedTxSingleThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedTxTimeoutSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedPreloadSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedPreloadLifecycleSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheSyncReplicatedPreloadSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheLockChangingTopologyTest.class, ignoredTests);

        //GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedMarshallerTxTest.class, ignoredTests);
        //GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedOnheapFullApiSelfTest.class, ignoredTests);
        //GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedOnheapMultiNodeFullApiSelfTest.class, ignoredTests);
        //GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedTxConcurrentGetTest.class, ignoredTests);
        //GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedTxReadTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheStartupInDeploymentModesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheConditionalDeploymentSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicEntryProcessorDeploymentSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheTransactionalEntryProcessorDeploymentSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheScanPredicateDeploymentSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCachePutArrayValueSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedEvictionEventSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedTxMultiThreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedPreloadEventsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedPreloadStartStopEventsSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteTxReentryNearSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTxReentryColocatedSelfTest.class, ignoredTests);

        // Test for byte array value special case.
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalByteArrayValuesSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearPartitionedP2PEnabledByteArrayValuesSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearPartitionedP2PDisabledByteArrayValuesSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedOnlyP2PEnabledByteArrayValuesSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedOnlyP2PDisabledByteArrayValuesSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedP2PEnabledByteArrayValuesSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedP2PDisabledByteArrayValuesSelfTest.class, ignoredTests);

        // Near-only cache.
        suite.addAll(IgniteCacheNearOnlySelfTestSuite.suite(ignoredTests));

        // Test cache with daemon nodes.
        GridTestUtils.addTestIfNeeded(suite, GridCacheDaemonNodeLocalSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDaemonNodePartitionedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheDaemonNodeReplicatedSelfTest.class, ignoredTests);

        // Write-behind.
        suite.addAll(IgniteCacheWriteBehindTestSuite.suite(ignoredTests));

        // Transform.
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedTransformWriteThroughBatchUpdateSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheEntryVersionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheVersionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheVersionTopologyChangeTest.class, ignoredTests);

        // Memory leak tests.
        GridTestUtils.addTestIfNeeded(suite, GridCacheReferenceCleanupSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReloadSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheMixedModeSelfTest.class, ignoredTests);

        // Cache interceptor tests.
        suite.addAll(IgniteCacheInterceptorSelfTestSuite.suite(ignoredTests));

        GridTestUtils.addTestIfNeeded(suite, IgniteTxGetAfterStopTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheAsyncOperationsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteTxRemoveTimeoutObjectsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTxRemoveTimeoutObjectsNearTest.class, ignoredTests);

        return suite;
    }
}
