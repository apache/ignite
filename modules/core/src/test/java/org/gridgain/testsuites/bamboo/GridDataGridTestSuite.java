/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.expiry.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.fair.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.cache.store.jdbc.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.cache.distributed.replicated.*;
import org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader.*;
import org.gridgain.grid.kernal.processors.cache.local.*;
import org.gridgain.grid.kernal.processors.dataload.*;
import org.gridgain.testsuites.*;

/**
 * Test suite.
 */
public class GridDataGridTestSuite extends TestSuite {
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

        suite.addTest(IgniteCacheExpiryPolicyTestSuite.suite());

        suite.addTestSuite(IgniteCacheAtomicInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicPrimaryWriteOrderInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicPrimaryWriteOrderWithStoreInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalInvokeTest.class);
        suite.addTestSuite(IgniteCacheAtomicLocalWithStoreInvokeTest.class);
        suite.addTestSuite(IgniteCacheTxInvokeTest.class);
        suite.addTestSuite(IgniteCacheTxNearEnabledInvokeTest.class);
        suite.addTestSuite(IgniteCacheTxLocalInvokeTest.class);

        // Affinity tests.
        suite.addTestSuite(GridCachePartitionFairAffinityNodesSelfTest.class);
        suite.addTestSuite(GridCacheAffinityBackupsSelfTest.class);

        // Swap tests.
        suite.addTestSuite(GridCacheSwapPreloadSelfTest.class);
        suite.addTestSuite(GridCacheSwapReloadSelfTest.class);

        // Common tests.
        suite.addTestSuite(GridCacheAffinityMapperSelfTest.class);
        suite.addTestSuite(GridCacheAffinityRoutingSelfTest.class);
        suite.addTestSuite(GridCacheMvccSelfTest.class);
        suite.addTestSuite(GridCacheMvccPartitionedSelfTest.class);
        suite.addTestSuite(GridCacheMvccManagerSelfTest.class);
//        suite.addTestSuite(GridCacheP2PUndeploySelfTest.class); TODO uncomment in DR branch.
        suite.addTestSuite(GridCacheConfigurationValidationSelfTest.class);
        suite.addTestSuite(GridCacheConfigurationConsistencySelfTest.class);
        suite.addTestSuite(GridCacheJdbcBlobStoreSelfTest.class);
        suite.addTestSuite(GridCacheJdbcBlobStoreMultithreadedSelfTest.class);
        suite.addTestSuite(GridCacheBalancingStoreSelfTest.class);
        suite.addTestSuite(GridCacheAffinityApiSelfTest.class);
        suite.addTestSuite(GridCacheStoreValueBytesSelfTest.class);
        suite.addTestSuite(GridDataLoaderProcessorSelfTest.class);
        suite.addTestSuite(GridDataLoaderImplSelfTest.class);
        suite.addTestSuite(GridCacheEntryMemorySizeSelfTest.class);
        suite.addTestSuite(GridCacheClearAllSelfTest.class);
        suite.addTestSuite(GridCacheGlobalClearAllSelfTest.class);
        suite.addTestSuite(GridCacheObjectToStringSelfTest.class);
        suite.addTestSuite(GridCacheLoadOnlyStoreAdapterSelfTest.class);
        suite.addTestSuite(GridCacheGetStoreErrorSelfTest.class);
        suite.addTestSuite(GridCacheAsyncOperationsLimitSelfTest.class);
        suite.addTestSuite(GridCacheTtlManagerSelfTest.class);
        suite.addTestSuite(GridCacheLifecycleAwareSelfTest.class);
        suite.addTestSuite(GridCacheStopSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearCacheSelfTest.class);
        suite.addTestSuite(GridCacheStorePutxSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapMultiThreadedUpdateSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapAtomicMultiThreadedUpdateSelfTest.class);
        suite.addTestSuite(GridCacheColocatedTxExceptionSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedTxExceptionSelfTest.class);
        suite.addTestSuite(GridCacheLocalTxExceptionSelfTest.class);
        suite.addTestSuite(GridCacheNearTxExceptionSelfTest.class);
        suite.addTestSuite(GridCacheColocatedTxStoreExceptionSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedTxStoreExceptionSelfTest.class);
        suite.addTestSuite(GridCacheLocalTxStoreExceptionSelfTest.class);
        suite.addTestSuite(GridCacheNearTxStoreExceptionSelfTest.class);
        suite.addTestSuite(GridCacheMissingCommitVersionSelfTest.class);
        suite.addTestSuite(GridCacheEntrySetIterationPreloadingSelfTest.class);
        suite.addTestSuite(GridCacheMixedPartitionExchangeSelfTest.class);
        suite.addTestSuite(GridCacheAtomicTimeoutSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapTieredEvictionAtomicSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapTieredEvictionSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapTieredAtomicSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapTieredSelfTest.class);

        // Local cache.
        suite.addTestSuite(GridCacheLocalProjectionSelfTest.class);
        suite.addTestSuite(GridCacheLocalAtomicProjectionSelfTest.class);
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

        // Partitioned cache.
        suite.addTestSuite(GridCachePartitionedGetSelfTest.class);
        suite.addTest(new TestSuite(GridCachePartitionedBasicApiTest.class));
        suite.addTest(new TestSuite(GridCacheNearMultiGetSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearJobExecutionSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedProjectionSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedOnlyProjectionSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearOneNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheAtomicNearMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearReadersSelfTest.class));
        suite.addTest(new TestSuite(GridCacheAtomicNearReadersSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAffinitySelfTest.class));
        suite.addTest(new TestSuite(GridCacheConsistentHashAffinityFunctionExcludeNeighborsSelfTest.class));
        suite.addTest(new TestSuite(GridCacheRendezvousAffinityFunctionExcludeNeighborsSelfTest.class));
        suite.addTest(new TestSuite(GridCacheRendezvousAffinityClientSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedProjectionAffinitySelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedBasicOpSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedBasicStoreSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedGetAndTransformStoreSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicGetAndTransformStoreSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedBasicStoreMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedNearDisabledBasicStoreMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedEventSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedLockSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedMultiNodeLockSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedMultiThreadedPutGetSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedNodeFailureSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedExplicitLockNodeFailureSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedTxSingleThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridCacheColocatedTxSingleThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedTxTimeoutSelfTest.class));
        suite.addTest(new TestSuite(GridCacheFinishPartitionsSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtEntrySelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtInternalEntrySelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtMappingSelfTest.class));
//        suite.addTest(new TestSuite(GridCachePartitionedTxMultiThreadedSelfTest.class)); TODO-gg-4066
        suite.addTest(new TestSuite(GridCacheDhtPreloadSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadOffHeapSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadBigDataSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadPutGetSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadDisabledSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridCacheColocatedPreloadRestartSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearPreloadRestartSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadUnloadSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAffinityFilterSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedPreloadLifecycleSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtPreloadDelayedSelfTest.class));
        suite.addTest(new TestSuite(GridPartitionedBackupLoadSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedLoadCacheSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtEvictionsDisabledSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearEvictionEventSelfTest.class));
        suite.addTest(new TestSuite(GridCacheAtomicNearEvictionEventSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtEvictionSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedEvictionSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtEvictionNearReadersSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDhtAtomicEvictionNearReadersSelfTest.class));
//        suite.addTest(new TestSuite(GridCachePartitionedTopologyChangeSelfTest.class)); TODO-gg-5489
        suite.addTest(new TestSuite(GridCachePartitionedPreloadEventsSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedUnloadEventsSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAffinityHashIdResolverSelfTest.class));
        suite.addTest(new TestSuite(GridCacheColocatedOptimisticTransactionSelfTest.class));
        suite.addTestSuite(GridCacheAtomicMessageCountSelfTest.class);
        suite.addTest(new TestSuite(GridCacheNearPartitionedClearSelfTest.class));

        suite.addTest(new TestSuite(GridCacheDhtExpiredEntriesPreloadSelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearExpiredEntriesPreloadSelfTest.class));
        suite.addTest(new TestSuite(GridCacheAtomicExpiredEntriesPreloadSelfTest.class));

        suite.addTest(new TestSuite(GridCacheReturnValueTransferSelfTest.class));
        suite.addTest(new TestSuite(GridCacheOffheapUpdateSelfTest.class));

        // TODO: GG-7242, GG-7243: Enabled when fixed.
//        suite.addTest(new TestSuite(GridCacheDhtRemoveFailureTest.class));
//        suite.addTest(new TestSuite(GridCacheNearRemoveFailureTest.class));
        // TODO: GG-7201: Enable when fixed.
        //suite.addTest(new TestSuite(GridCacheDhtAtomicRemoveFailureTest.class));

        suite.addTest(new TestSuite(GridCacheNearPrimarySyncSelfTest.class));
        suite.addTest(new TestSuite(GridCacheColocatedPrimarySyncSelfTest.class));

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
        suite.addTestSuite(GridCacheReplicatedProjectionSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedTxSingleThreadedSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedTxTimeoutSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedPreloadSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedPreloadOffHeapSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedPreloadLifecycleSelfTest.class);
        suite.addTestSuite(GridCacheSyncReplicatedPreloadSelfTest.class);
        // suite.addTestSuite(GridCacheReplicatedFailoverSelfTest.class); TODO: uncomment when fix GG-2239
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
        suite.addTest(GridCacheNearOnlySelfTestSuite.suite());

        // Test cache with daemon nodes.
        suite.addTestSuite(GridCacheDaemonNodeLocalSelfTest.class);
        suite.addTestSuite(GridCacheDaemonNodePartitionedSelfTest.class);
        suite.addTestSuite(GridCacheDaemonNodeReplicatedSelfTest.class);

        // Write-behind.
        suite.addTest(GridCacheWriteBehindTestSuite.suite());

        // Transform.
        suite.addTestSuite(GridCachePartitionedTransformWriteThroughBatchUpdateSelfTest.class);
        suite.addTestSuite(GridCacheIncrementTransformTest.class);

        suite.addTestSuite(GridCacheEntryVersionSelfTest.class);
        suite.addTestSuite(GridCacheVersionSelfTest.class);

        // Memory leak tests.
        suite.addTestSuite(GridCacheReferenceCleanupSelfTest.class);
        suite.addTestSuite(GridCacheReloadSelfTest.class);

        // Group locking.
        suite.addTest(GridCacheGroupLockSelfTestSuite.suite());

        // Full API.
        suite.addTest(GridCacheFullApiSelfTestSuite.suite());
        suite.addTestSuite(GridCacheMixedModeSelfTest.class);

        // Cache metrics.
        suite.addTest(GridCacheMetricsSelfTestSuite.suite());

        // Eviction.
        suite.addTest(GridCacheEvictionSelfTestSuite.suite());

        // Iterators.
        suite.addTest(GridCacheIteratorsSelfTestSuite.suite());

        // Add tx recovery test suite.
        suite.addTest(GridCacheTxRecoverySelfTestSuite.suite());

        // Cache interceptor tests.
        suite.addTest(GridCacheInterceptorSelfTestSuite.suite());

        // Multi node update.
        suite.addTestSuite(GridCacheMultinodeUpdateSelfTest.class);
        // TODO: GG-5353.
        // suite.addTestSuite(GridCacheMultinodeUpdateNearEnabledSelfTest.class);
        // suite.addTestSuite(GridCacheMultinodeUpdateNearEnabledNoBackupsSelfTest.class);
        suite.addTestSuite(GridCacheMultinodeUpdateAtomicSelfTest.class);
        suite.addTestSuite(GridCacheMultinodeUpdateAtomicNearEnabledSelfTest.class);

        return suite;
    }
}
