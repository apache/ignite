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
import org.apache.ignite.internal.processors.cache.persistence.standbycluster.extended.GridActivateExtensionTest;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * Test suite for cache API.
 */
public class IgniteCacheFullApiSelfTestSuite extends TestSuite {
    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(GridAbstractTest.PERSISTENCE_IN_TESTS_IS_ALLOWED_PROPERTY, "false");

        TestSuite suite = new TestSuite("Cache Full API Test Suite");

        // One node.
        suite.addTestSuite(GridCacheLocalFullApiSelfTest.class);
        suite.addTestSuite(GridCacheLocalAtomicFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedFilteredPutSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedAtomicFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearEnabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicOnheapFullApiSelfTest.class);

        suite.addTestSuite(GridCachePartitionedOnheapFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedAtomicOnheapFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledOnheapFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledAtomicOnheapFullApiSelfTest.class);

        // No primary.
        suite.addTestSuite(GridCachePartitionedClientOnlyNoPrimaryFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearOnlyNoPrimaryFullApiSelfTest.class);

        // Multi-node.
        suite.addTestSuite(GridCacheReplicatedMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedMultiNodeP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedAtomicMultiNodeFullApiSelfTest.class);

        suite.addTestSuite(GridCachePartitionedMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedCopyOnReadDisabledMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicCopyOnReadDisabledMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedMultiNodeP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicMultiNodeP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearEnabledMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(CachePartitionedMultiNodeLongTxTimeoutFullApiTest.class);
        suite.addTestSuite(CachePartitionedMultiNodeLongTxTimeout2FullApiTest.class);
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

        suite.addTestSuite(CacheReplicatedRendezvousAffinityExcludeNeighborsMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(CacheReplicatedRendezvousAffinityMultiNodeFullApiSelfTest.class);

        suite.addTestSuite(GridCacheNearReloadAllSelfTest.class);
        suite.addTestSuite(GridCacheColocatedReloadAllSelfTest.class);
        suite.addTestSuite(GridCacheAtomicReloadAllSelfTest.class);
        suite.addTestSuite(GridCacheNearTxMultiNodeSelfTest.class);
        suite.addTestSuite(GridCachePartitionedMultiNodeCounterSelfTest.class);

        suite.addTestSuite(GridCachePartitionedOnheapMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedAtomicOnheapMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledOnheapMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledAtomicOnheapMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicOnheapMultiNodeFullApiSelfTest.class);

        // Multithreaded.
        suite.addTestSuite(GridCacheLocalFullApiMultithreadedSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedFullApiMultithreadedSelfTest.class);
        suite.addTestSuite(GridCachePartitionedFullApiMultithreadedSelfTest.class);

        // Other.
        suite.addTestSuite(GridCacheClearSelfTest.class);

        suite.addTestSuite(GridCacheLocalWithGroupFullApiSelfTest.class);
        suite.addTestSuite(GridCacheLocalAtomicWithGroupFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicMultiNodeWithGroupFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearEnabledMultiNodeWithGroupFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedMultiNodeWithGroupFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledMultiNodeWithGroupFullApiSelfTest.class);

        //suite.addTestSuite(GridActivateExtensionTest.class);

        return suite;
    }
}
