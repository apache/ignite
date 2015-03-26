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

import junit.framework.*;
import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.fair.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.context.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.distributed.replicated.*;
import org.apache.ignite.internal.processors.cache.distributed.replicated.preloader.*;
import org.apache.ignite.internal.processors.cache.integration.*;
import org.apache.ignite.internal.processors.cache.local.*;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("IgniteCache Test Suite");

        suite.addTestSuite(IgniteCacheEntryListenerAtomicTest.class);
        suite.addTestSuite(IgniteCacheEntryListenerAtomicReplicatedTest.class);
        suite.addTestSuite(IgniteCacheEntryListenerAtomicLocalTest.class);
        suite.addTestSuite(IgniteCacheEntryListenerTxTest.class);
        suite.addTestSuite(IgniteCacheEntryListenerTxReplicatedTest.class);
        suite.addTestSuite(IgniteCacheEntryListenerTxLocalTest.class);
        suite.addTestSuite(IgniteCacheEntryListenerEagerTtlDisabledTest.class);

        suite.addTestSuite(IgniteClientAffinityAssignmentSelfTest.class);

        suite.addTestSuite(IgniteCacheAtomicInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicPrimaryWriteOrderInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicPrimaryWriteOrderWithStoreInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalWithStoreInvokeTest.class);
        suite.addTestSuite(IgniteCacheTxInvokeTest.class);
        suite.addTestSuite(IgniteCacheTxNearEnabledInvokeTest.class);
        suite.addTestSuite(IgniteCacheTxLocalInvokeTest.class);
        suite.addTestSuite(IgniteCrossCacheTxStoreSelfTest.class);

        suite.addTestSuite(IgnitePutAllLargeBatchSelfTest.class);
        suite.addTestSuite(IgnitePutAllUpdateNonPreloadedPartitionSelfTest.class);

        // User's class loader tests.
        suite.addTestSuite(IgniteCacheAtomicExecutionContextTest.class);
        suite.addTestSuite(IgniteCachePartitionedExecutionContextTest.class);
        suite.addTestSuite(IgniteCacheReplicatedExecutionContextTest.class);
        suite.addTestSuite(IgniteCacheTxExecutionContextTest.class);
        suite.addTestSuite(IgniteCacheContinuousExecutionContextTest.class);
        suite.addTestSuite(IgniteCacheIsolatedExecutionContextTest.class);
        suite.addTestSuite(IgniteCacheP2PDisableExecutionContextTest.class);
        suite.addTestSuite(IgniteCachePrivateExecutionContextTest.class);
        suite.addTestSuite(IgniteCacheSharedExecutionContextTest.class);

        // Affinity tests.
        suite.addTestSuite(GridFairAffinityFunctionNodesSelfTest.class);
        suite.addTestSuite(GridCacheAffinityBackupsSelfTest.class);
        suite.addTestSuite(IgniteCacheAffinitySelfTest.class);

        // Swap tests.
        suite.addTestSuite(GridCacheSwapPreloadSelfTest.class);
        suite.addTestSuite(GridCacheSwapReloadSelfTest.class);

        // Heuristic exception handling. TODO IGNITE-257
//        suite.addTestSuite(GridCacheColocatedTxExceptionSelfTest.class);
//        suite.addTestSuite(GridCacheReplicatedTxExceptionSelfTest.class);
//        suite.addTestSuite(GridCacheLocalTxExceptionSelfTest.class);
//        suite.addTestSuite(GridCacheNearTxExceptionSelfTest.class);
//        suite.addTestSuite(GridCacheStopSelfTest.class); TODO IGNITE-257

        // Local cache.
        suite.addTestSuite(GridCacheLocalBasicApiSelfTest.class);
        suite.addTestSuite(GridCacheLocalBasicStoreSelfTest.class);
        suite.addTestSuite(GridCacheLocalAtomicBasicStoreSelfTest.class);
        suite.addTestSuite(GridCacheLocalGetAndTransformStoreSelfTest.class);
        suite.addTestSuite(GridCacheLocalAtomicGetAndTransformStoreSelfTest.class);
        suite.addTestSuite(GridCacheLocalLoadAllSelfTest.class);
        suite.addTestSuite(GridCacheLocalLockSelfTest.class);
        suite.addTestSuite(GridCacheLocalMultithreadedSelfTest.class);
        suite.addTestSuite(GridCacheLocalTxSingleThreadedSelfTest.class);
        suite.addTestSuite(GridCacheLocalTxTimeoutSelfTest.class);
        suite.addTestSuite(GridCacheLocalEventSelfTest.class);
        suite.addTestSuite(GridCacheLocalEvictionEventSelfTest.class);
        suite.addTestSuite(GridCacheVariableTopologySelfTest.class);
        suite.addTestSuite(GridCacheLocalTxMultiThreadedSelfTest.class);
        suite.addTestSuite(GridCacheTransformEventSelfTest.class);

        // Value consistency tests.
        suite.addTestSuite(GridCacheValueConsistencyAtomicSelfTest.class);
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

        // TODO: GG-7437.
        // suite.addTestSuite(GridCacheReplicatedInvalidateSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedLockSelfTest.class);
        // TODO: enable when GG-7437 is fixed.
        //suite.addTestSuite(GridCacheReplicatedMultiNodeLockSelfTest.class);
        //suite.addTestSuite(GridCacheReplicatedMultiNodeSelfTest.class);
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
        // TODO: GG-7569.
        // suite.addTestSuite(GridCacheReplicatedTxMultiThreadedSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedPreloadEventsSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedPreloadStartStopEventsSelfTest.class);
        // TODO: GG-7434
        // suite.addTestSuite(GridReplicatedTxPreloadTest.class);

        suite.addTestSuite(IgniteTxReentryNearSelfTest.class);
        suite.addTestSuite(IgniteTxReentryColocatedSelfTest.class);

        suite.addTestSuite(GridCacheOrderedPreloadingSelfTest.class);

        // Test for byte array value special case.
//        suite.addTestSuite(GridCacheLocalByteArrayValuesSelfTest.class);
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

        // Eviction.
        suite.addTest(IgniteCacheEvictionSelfTestSuite.suite());

        // Iterators.
        suite.addTest(IgniteCacheIteratorsSelfTestSuite.suite());

        // Add tx recovery test suite.
        suite.addTest(IgniteCacheTxRecoverySelfTestSuite.suite());

        // Cache interceptor tests.
        suite.addTest(IgniteCacheInterceptorSelfTestSuite.suite());

        // Multi node update.
        suite.addTestSuite(GridCacheMultinodeUpdateSelfTest.class);
        // TODO: GG-5353.
        // suite.addTestSuite(GridCacheMultinodeUpdateNearEnabledSelfTest.class);
        // suite.addTestSuite(GridCacheMultinodeUpdateNearEnabledNoBackupsSelfTest.class);
        suite.addTestSuite(GridCacheMultinodeUpdateAtomicSelfTest.class);
        suite.addTestSuite(GridCacheMultinodeUpdateAtomicNearEnabledSelfTest.class);

        suite.addTestSuite(IgniteCacheAtomicLoadAllTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalLoadAllTest.class);
        suite.addTestSuite(IgniteCacheTxLoadAllTest.class);
        suite.addTestSuite(IgniteCacheTxLocalLoadAllTest.class);

        suite.addTestSuite(IgniteCacheAtomicLoaderWriterTest.class);
        suite.addTestSuite(IgniteCacheTxLoaderWriterTest.class);

        suite.addTestSuite(IgniteCacheAtomicStoreSessionTest.class);
        suite.addTestSuite(IgniteCacheTxStoreSessionTest.class);
        suite.addTestSuite(IgniteCacheAtomicStoreSessionWriteBehindTest.class);
        suite.addTestSuite(IgniteCacheTxStoreSessionWriteBehindTest.class);

        suite.addTestSuite(IgniteCacheAtomicNoReadThroughTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledNoReadThroughTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalNoReadThroughTest.class);
        suite.addTestSuite(IgniteCacheTxNoReadThroughTest.class);
        suite.addTestSuite(IgniteCacheTxNearEnabledNoReadThroughTest.class);
        suite.addTestSuite(IgniteCacheTxLocalNoReadThroughTest.class);

        suite.addTestSuite(IgniteCacheAtomicNoLoadPreviousValueTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledNoLoadPreviousValueTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalNoLoadPreviousValueTest.class);
        suite.addTestSuite(IgniteCacheTxNoLoadPreviousValueTest.class);
        suite.addTestSuite(IgniteCacheTxNearEnabledNoLoadPreviousValueTest.class);
        suite.addTestSuite(IgniteCacheTxLocalNoLoadPreviousValueTest.class);

        suite.addTestSuite(IgniteCacheAtomicNoWriteThroughTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledNoWriteThroughTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalNoWriteThroughTest.class);
        suite.addTestSuite(IgniteCacheTxNoWriteThroughTest.class);
        suite.addTestSuite(IgniteCacheTxNearEnabledNoWriteThroughTest.class);
        suite.addTestSuite(IgniteCacheTxLocalNoWriteThroughTest.class);

        suite.addTestSuite(IgniteCacheAtomicPeekModesTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearPeekModesTest.class);
        suite.addTestSuite(IgniteCacheAtomicReplicatedPeekModesTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalPeekModesTest.class);
        suite.addTestSuite(IgniteCacheTxPeekModesTest.class);
        suite.addTestSuite(IgniteCacheTxNearPeekModesTest.class);
        suite.addTestSuite(IgniteCacheTxLocalPeekModesTest.class);
        suite.addTestSuite(IgniteCacheTxReplicatedPeekModesTest.class);

        // TODO: IGNITE-114.
        // suite.addTestSuite(IgniteCacheInvokeReadThroughTest.class);
        // suite.addTestSuite(GridCacheVersionMultinodeTest.class);

        suite.addTestSuite(IgniteCacheNearReadCommittedTest.class);
        suite.addTestSuite(IgniteCacheAtomicCopyOnReadDisabledTest.class);
        suite.addTestSuite(IgniteCacheTxCopyOnReadDisabledTest.class);

        // TODO: IGNITE-477.
        // suite.addTestSuite(IgniteCacheTxPreloadNoWriteTest.class);

        suite.addTestSuite(IgniteDynamicCacheStartSelfTest.class);
        suite.addTestSuite(IgniteCacheDynamicStopSelfTest.class);

        suite.addTestSuite(GridCacheTxLoadFromStoreOnLockSelfTest.class);

        suite.addTestSuite(GridCacheMarshallingNodeJoinSelfTest.class);

        return suite;
    }
}
