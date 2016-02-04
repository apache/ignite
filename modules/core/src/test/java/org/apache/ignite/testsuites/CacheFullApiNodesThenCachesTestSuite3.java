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
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicPrimaryWriteOrderReloadAllSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicReloadAllSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedReloadAllSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledAtomicOffHeapTieredMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOffHeapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOffHeapTieredMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicClientOnlyFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEnabledFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicOffHeapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicOffHeapTieredMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWriteOrderFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWrityOrderOffHeapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWrityOrderOffHeapTieredMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOnlyFairAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearReloadAllSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFullApiMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeCounterSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOffHeapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOffHeapTieredMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedFullApiMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedOffHeapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedOffHeapTieredMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalFullApiMultithreadedSelfTest;

/**
 * Test suite for cache API.
 */
public class CacheFullApiNodesThenCachesTestSuite3 extends TestSuite {
    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty("cache.start.mode", "NODES_THEN_CACHES");

        TestSuite suite = new TestSuite("Cache Full API Test Suite");

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

        // Multithreaded.
        suite.addTestSuite(GridCacheLocalFullApiMultithreadedSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedFullApiMultithreadedSelfTest.class);
        suite.addTestSuite(GridCachePartitionedFullApiMultithreadedSelfTest.class);

        // Other.
        suite.addTestSuite(GridCacheClearSelfTest.class);

        return suite;
    }
}
