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
import org.apache.ignite.cache.store.*;
import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.datastreamer.*;

/**
 * Test suite.
 */
public class IgniteCacheCommonTestSuite extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Cache Common Test Suite");

        suite.addTestSuite(GridCacheConcurrentMapSelfTest.class);
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
        suite.addTestSuite(CacheJdbcPojoStoreTest.class);
        suite.addTestSuite(CacheJdbcPojoStoreMultitreadedSelfTest.class);
        suite.addTestSuite(GridCacheBalancingStoreSelfTest.class);
        suite.addTestSuite(GridCacheAffinityApiSelfTest.class);
        suite.addTestSuite(GridCacheStoreValueBytesSelfTest.class);
        suite.addTestSuite(DataStreamProcessorSelfTest.class);
        suite.addTestSuite(DataStreamerImplSelfTest.class);
        suite.addTestSuite(GridCacheEntryMemorySizeSelfTest.class);
        suite.addTestSuite(GridCacheClearAllSelfTest.class);
        suite.addTestSuite(GridCacheObjectToStringSelfTest.class);
        suite.addTestSuite(GridCacheLoadOnlyStoreAdapterSelfTest.class);
        suite.addTestSuite(GridCacheGetStoreErrorSelfTest.class);
        suite.addTestSuite(GridCacheAsyncOperationsLimitSelfTest.class);
        suite.addTestSuite(GridCacheTtlManagerSelfTest.class);
        suite.addTestSuite(GridCacheLifecycleAwareSelfTest.class);
        suite.addTestSuite(IgniteCacheAtomicStopBusySelfTest.class);
        // suite.addTestSuite(IgniteCacheTransactionalStopBusySelfTest.class); TODO Ignite-257.
        suite.addTestSuite(GridCacheAtomicNearCacheSelfTest.class);
        suite.addTestSuite(CacheAtomicNearUpdateTopologyChangeTest.class);
        suite.addTestSuite(CacheTxNearUpdateTopologyChangeTest.class);
        suite.addTestSuite(GridCacheStorePutxSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapMultiThreadedUpdateSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapAtomicMultiThreadedUpdateSelfTest.class);
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
        suite.addTestSuite(GridCacheGlobalLoadTest.class);
        suite.addTestSuite(GridCachePartitionedLocalStoreSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedLocalStoreSelfTest.class);
        suite.addTestSuite(GridCachePartitionedOffHeapLocalStoreSelfTest.class);
        suite.addTestSuite(GridCacheTxPartitionedLocalStoreSelfTest.class);
        suite.addTestSuite(IgniteCacheSystemTransactionsSelfTest.class);

        return suite;
    }
}
