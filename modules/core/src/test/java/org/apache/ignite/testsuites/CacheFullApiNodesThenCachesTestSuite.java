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
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicNearEnabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicNearEnabledPrimaryWriteOrderFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicPrimaryWriteOrderFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOffHeapTieredAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOffHeapTieredFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicOffHeapTieredFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWriteOrderOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWriteOrderOffHeapTieredFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedClientOnlyNoPrimaryFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFilteredPutSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNearOnlyNoPrimaryFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOffHeapTieredFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedOffHeapTieredFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicOffHeapTieredFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalOffHeapFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalOffHeapTieredFullApiSelfTest;

/**
 * Test suite for cache API.
 */
public class CacheFullApiNodesThenCachesTestSuite extends TestSuite {
    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty("cache.start.mode", "NODES_THEN_CACHES");

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

        return suite;
    }
}
