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
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.distributed.replicated.*;
import org.apache.ignite.internal.processors.cache.local.*;

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

        suite.addTestSuite(GridCachePartitionedNearDisabledMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledMultiNodeP2PDisabledFullApiSelfTest.class);

        suite.addTestSuite(GridCacheNearOnlyMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheNearOnlyMultiNodeP2PDisabledFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedNearOnlyMultiNodeFullApiSelfTest.class);

        suite.addTestSuite(GridCacheAtomicClientOnlyMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicClientOnlyMultiNodeP2PDisabledFullApiSelfTest.class);

        suite.addTestSuite(GridCacheAtomicNearOnlyMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheAtomicNearOnlyMultiNodeP2PDisabledFullApiSelfTest.class);

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

//        Multithreaded // TODO: GG-708
//        suite.addTestSuite(GridCacheLocalFullApiMultithreadedSelfTest.class);
//        suite.addTestSuite(GridCacheReplicatedFullApiMultithreadedSelfTest.class);
//        suite.addTestSuite(GridCachePartitionedFullApiMultithreadedSelfTest.class);

        return suite;
    }
}
