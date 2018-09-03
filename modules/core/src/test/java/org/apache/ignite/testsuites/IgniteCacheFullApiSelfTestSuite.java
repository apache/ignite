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
import org.apache.ignite.internal.processors.cache.GridCacheClearSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicNearEnabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicNearEnabledPrimaryWriteOrderFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicPrimaryWriteOrderFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicPrimaryWriteOrderReloadAllSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicReloadAllSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedReloadAllSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledAtomicOffHeapTieredMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOffHeapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOffHeapTieredAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOffHeapTieredFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOffHeapTieredMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.CachePartitionedMultiNodeLongTxTimeoutFullApiTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicClientOnlyFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicClientOnlyMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicClientOnlyMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicCopyOnReadDisabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicLateAffDisabledPrimaryWriteOrderMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEnabledFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEnabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEnabledPrimaryWriteOrderMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearOnlyMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearOnlyMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicOffHeapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicOffHeapTieredFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicOffHeapTieredMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWriteOrderFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWriteOrderMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWriteOrderMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWriteOrderNoStripedPoolMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWriteOrderOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWriteOrderOffHeapTieredFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWrityOrderOffHeapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWrityOrderOffHeapTieredMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOnlyFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOnlyMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOnlyMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearReloadAllSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedClientOnlyNoPrimaryFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedCopyOnReadDisabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFilteredPutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFullApiMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedLateAffDisabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeCounterSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.CachePartitionedNearEnabledMultiNodeLongTxTimeoutFullApiTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNearOnlyNoPrimaryFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNoStripedPoolMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOffHeapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOffHeapTieredFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOffHeapTieredMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.CacheReplicatedFairAffinityExcludeNeighborsMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.CacheReplicatedFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.CacheReplicatedRendezvousAffinityExcludeNeighborsMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.CacheReplicatedRendezvousAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCachePartitionedFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicPrimaryWriteOrderMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedFullApiMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedNearOnlyMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedOffHeapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedOffHeapTieredFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedOffHeapTieredMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicOffHeapTieredFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalFullApiMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalOffHeapTieredFullApiSelfTest;

/**
 * Test suite for cache API.
 */
public class IgniteCacheFullApiSelfTestSuite extends TestSuite {
    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache Full API Test Suite");

        // One node.
        suite.addTestSuite(GridCacheLocalFullApiSelfTest.class);
        suite.addTestSuite(GridCacheLocalAtomicFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedFilteredPutSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedAtomicFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearEnabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearEnabledPrimaryWriteOrderFullApiSelfTest.class);

        // No primary.
        suite.addTestSuite(GridCachePartitionedClientOnlyNoPrimaryFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearOnlyNoPrimaryFullApiSelfTest.class);

        // One node with off-heap values.
        suite.addTestSuite(GridCacheLocalOffHeapFullApiSelfTest.class);
        suite.addTestSuite(GridCacheLocalAtomicOffHeapFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedOffHeapFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedOffHeapFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicOffHeapFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderOffHeapFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledOffHeapFullApiSelfTest.class);

        // One node with off-heap tiered mode.
        suite.addTestSuite(GridCacheLocalOffHeapTieredFullApiSelfTest.class);
        suite.addTestSuite(GridCacheLocalAtomicOffHeapTieredFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedOffHeapTieredFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedOffHeapTieredFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicOffHeapTieredFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderOffHeapTieredFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledOffHeapTieredFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledOffHeapTieredAtomicFullApiSelfTest.class);

        // Multi-node.
        suite.addTestSuite(GridCacheReplicatedMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedMultiNodeP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedAtomicMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedAtomicPrimaryWriteOrderMultiNodeFullApiSelfTest.class);

        suite.addTestSuite(GridCachePartitionedMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedCopyOnReadDisabledMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicCopyOnReadDisabledMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedMultiNodeP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicMultiNodeP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderMultiNodeP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearEnabledMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearEnabledPrimaryWriteOrderMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(CachePartitionedMultiNodeLongTxTimeoutFullApiTest.class);
        suite.addTestSuite(CachePartitionedNearEnabledMultiNodeLongTxTimeoutFullApiTest.class);

        suite.addTestSuite(GridCachePartitionedNearDisabledMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledMultiNodeP2PDisabledFullApiSelfTest.class);

        suite.addTestSuite(GridCacheNearOnlyMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheNearOnlyMultiNodeP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedNearOnlyMultiNodeFullApiSelfTest.class);

        suite.addTestSuite(GridCacheAtomicClientOnlyMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicClientOnlyMultiNodeP2PDisabledFullApiSelfTest.class);

        suite.addTestSuite(GridCacheAtomicNearOnlyMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearOnlyMultiNodeP2PDisabledFullApiSelfTest.class);

        suite.addTestSuite(CacheReplicatedFairAffinityExcludeNeighborsMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(CacheReplicatedFairAffinityMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(CacheReplicatedRendezvousAffinityExcludeNeighborsMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(CacheReplicatedRendezvousAffinityMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedFairAffinityMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledFairAffinityMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicFairAffinityMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearEnabledFairAffinityMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderFairAffinityMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheNearOnlyFairAffinityMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicClientOnlyFairAffinityMultiNodeFullApiSelfTest.class);

        suite.addTestSuite(GridCacheNearReloadAllSelfTest.class);
        suite.addTestSuite(GridCacheColocatedReloadAllSelfTest.class);
        suite.addTestSuite(GridCacheAtomicReloadAllSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderReloadAllSelfTest.class);
        suite.addTestSuite(GridCacheNearTxMultiNodeSelfTest.class);
        suite.addTestSuite(GridCachePartitionedMultiNodeCounterSelfTest.class);

        // Multi-node with off-heap values.
        suite.addTestSuite(GridCacheReplicatedOffHeapMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedOffHeapMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicOffHeapMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWrityOrderOffHeapMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledOffHeapMultiNodeFullApiSelfTest.class);

        // Multi-node with off-heap tiered mode.
        suite.addTestSuite(GridCacheReplicatedOffHeapTieredMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedOffHeapTieredMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicOffHeapTieredMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWrityOrderOffHeapTieredMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledOffHeapTieredMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledAtomicOffHeapTieredMultiNodeFullApiSelfTest.class);

        // Old affinity assignment mode.
        suite.addTestSuite(GridCachePartitionedLateAffDisabledMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicLateAffDisabledPrimaryWriteOrderMultiNodeFullApiSelfTest.class);

        // Multithreaded.
        suite.addTestSuite(GridCacheLocalFullApiMultithreadedSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedFullApiMultithreadedSelfTest.class);
        suite.addTestSuite(GridCachePartitionedFullApiMultithreadedSelfTest.class);

        // Disabled striped pool.
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderNoStripedPoolMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNoStripedPoolMultiNodeFullApiSelfTest.class);

        // Other.
        suite.addTestSuite(GridCacheClearSelfTest.class);

        return suite;
    }
}
