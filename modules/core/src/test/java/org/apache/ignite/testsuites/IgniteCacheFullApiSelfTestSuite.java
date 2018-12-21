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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.GridCacheClearSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicNearEnabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicReloadAllSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedReloadAllSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledAtomicOnheapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledAtomicOnheapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledMultiNodeWithGroupFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOnheapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOnheapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.CachePartitionedMultiNodeLongTxTimeout2FullApiTest;
import org.apache.ignite.internal.processors.cache.distributed.near.CachePartitionedMultiNodeLongTxTimeoutFullApiTest;
import org.apache.ignite.internal.processors.cache.distributed.near.CachePartitionedNearEnabledMultiNodeLongTxTimeoutFullApiTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicClientOnlyMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicClientOnlyMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicCopyOnReadDisabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicMultiNodeWithGroupFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEnabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEnabledMultiNodeWithGroupFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearOnlyMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearOnlyMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicOnheapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicOnheapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOnlyMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOnlyMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearReloadAllSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAtomicOnheapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAtomicOnheapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedClientOnlyNoPrimaryFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedCopyOnReadDisabledMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFilteredPutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFullApiMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeCounterSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeWithGroupFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNearOnlyNoPrimaryFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOnheapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOnheapMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.CacheReplicatedRendezvousAffinityExcludeNeighborsMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.CacheReplicatedRendezvousAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedFullApiMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedNearOnlyMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicWithGroupFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalFullApiMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalWithGroupFullApiSelfTest;

/**
 * Test suite for cache API.
 */
public class IgniteCacheFullApiSelfTestSuite extends TestSuite {
    /**
     * @return Cache API test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Cache Full API Test Suite");

        // One node.
        suite.addTest(new JUnit4TestAdapter(GridCacheLocalFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheLocalAtomicFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedFilteredPutSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedAtomicFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicNearEnabledFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicOnheapFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedOnheapFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicOnheapFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledOnheapFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledAtomicOnheapFullApiSelfTest.class));

        // No primary.
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedClientOnlyNoPrimaryFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearOnlyNoPrimaryFullApiSelfTest.class));

        // Multi-node.
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedMultiNodeP2PDisabledFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedAtomicMultiNodeFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedCopyOnReadDisabledMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicCopyOnReadDisabledMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedMultiNodeP2PDisabledFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicMultiNodeP2PDisabledFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicNearEnabledMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CachePartitionedMultiNodeLongTxTimeoutFullApiTest.class));
        suite.addTest(new JUnit4TestAdapter(CachePartitionedMultiNodeLongTxTimeout2FullApiTest.class));
        suite.addTest(new JUnit4TestAdapter(CachePartitionedNearEnabledMultiNodeLongTxTimeoutFullApiTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledMultiNodeP2PDisabledFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheNearOnlyMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheNearOnlyMultiNodeP2PDisabledFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedNearOnlyMultiNodeFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicClientOnlyMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicClientOnlyMultiNodeP2PDisabledFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicNearOnlyMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicNearOnlyMultiNodeP2PDisabledFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheReplicatedRendezvousAffinityExcludeNeighborsMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheReplicatedRendezvousAffinityMultiNodeFullApiSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheNearReloadAllSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheColocatedReloadAllSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicReloadAllSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheNearTxMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedMultiNodeCounterSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedOnheapMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedAtomicOnheapMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledOnheapMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledAtomicOnheapMultiNodeFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicOnheapMultiNodeFullApiSelfTest.class));

        // Multithreaded.
        suite.addTest(new JUnit4TestAdapter(GridCacheLocalFullApiMultithreadedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedFullApiMultithreadedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedFullApiMultithreadedSelfTest.class));

        // Other.
        suite.addTest(new JUnit4TestAdapter(GridCacheClearSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheLocalWithGroupFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheLocalAtomicWithGroupFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicMultiNodeWithGroupFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicNearEnabledMultiNodeWithGroupFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedMultiNodeWithGroupFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledMultiNodeWithGroupFullApiSelfTest.class));

        //suite.addTest(new JUnit4TestAdapter(GridActivateExtensionTest.class));

        return suite;
    }
}
