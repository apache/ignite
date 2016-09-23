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
import org.apache.ignite.cache.affinity.fair.FairAffinityFunctionBackupFilterSelfTest;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunctionExcludeNeighborsSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionBackupFilterSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunctionExcludeNeighborsSelfTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalanceMultipleConnectionsTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalanceTest;
import org.apache.ignite.internal.processors.cache.CacheConfigurationLeakTest;
import org.apache.ignite.internal.processors.cache.CacheDhtLocalPartitionAfterRemoveSelfTest;
import org.apache.ignite.internal.processors.cache.CacheEnumOperationsSingleNodeTest;
import org.apache.ignite.internal.processors.cache.CacheEnumOperationsTest;
import org.apache.ignite.internal.processors.cache.CrossCacheTxRandomOperationsTest;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicMessageCountSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheFinishPartitionsSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffheapUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedGetSelfTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedProjectionAffinitySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheVariableTopologySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteAtomicCacheEntryProcessorNodeJoinTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryProcessorNodeJoinTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheIncrementTxTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePartitionMapUpdateTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheAndNodeStop;
import org.apache.ignite.internal.processors.cache.distributed.CacheLoadingConcurrentGridStartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheLockReleaseNodeLeaveTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePartitionNotLoadedEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePartitionedNearDisabledTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTransformEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheClientNodeChangingTopologyTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheClientNodePartitionsExchangeTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheNearOffheapGetSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheServerNodeConcurrentStart;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicExpiredEntriesPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedOptimisticTransactionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedPreloadRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedPrimarySyncSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtAtomicEvictionNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtEntrySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtEvictionNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtEvictionsDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtExpiredEntriesPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtMappingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadBigDataSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadDelayedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadOffHeapSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadPutGetSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadStartStopSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadUnloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedPreloadEventsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedTopologyChangeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedUnloadEventsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEvictionEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearEvictionEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearExpiredEntriesPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearJobExecutionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearMultiGetSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOneNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearPartitionedClearSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearPreloadRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearPrimarySyncSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearReaderPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxForceKeyTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAffinityHashIdResolverSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAffinitySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAtomicGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedBasicApiTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedBasicOpSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedBasicStoreMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedBasicStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedExplicitLockNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedLoadCacheSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeLockSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiThreadedPutGetSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNearDisabledBasicStoreMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedPreloadLifecycleSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheRendezvousAffinityClientSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridPartitionedBackupLoadSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.NearCacheSyncUpdateTest;
import org.apache.ignite.internal.processors.cache.distributed.near.NoneRebalanceModeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedJobExecutionTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicBasicStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalBasicApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalBasicStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalEventSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalEvictionEventSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalIsolatedNodesSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalLoadAllSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalLockSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalTxMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalTxSingleThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalTxTimeoutSelfTest;
import org.apache.ignite.internal.processors.continuous.IgniteNoCustomEventsOnNodeStart;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite2 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("IgniteCache Test Suite part 2");

        suite.addTestSuite(IgniteCommunicationBalanceTest.class);
        suite.addTestSuite(IgniteCommunicationBalanceMultipleConnectionsTest.class);
        suite.addTestSuite(IgniteCommunicationBalanceTest.class);
        suite.addTestSuite(IgniteCommunicationBalanceMultipleConnectionsTest.class);
        suite.addTestSuite(IgniteCommunicationBalanceTest.class);
        suite.addTestSuite(IgniteCommunicationBalanceMultipleConnectionsTest.class);

        return suite;
    }
}
