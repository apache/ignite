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

import java.util.HashSet;
import java.util.Set;
import junit.framework.TestSuite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityRoutingSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalWithStoreInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicNearEnabledInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicWithStoreInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicLocalTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicTest;
import org.apache.ignite.internal.processors.cache.IgniteClientAffinityAssignmentSelfTest;
import org.apache.ignite.internal.processors.cache.binary.CacheKeepBinaryWithInterceptorTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAffinityRoutingBinarySelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheAtomicPartitionedOnlyBinaryMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.expiry.IgniteCacheAtomicLocalExpiryPolicyTest;

/**
 * Test suite.
 */
public class IgniteCacheMvccTestSuite1 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     */
    public static TestSuite suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        TestSuite suite = new TestSuite("IgniteCache Mvcc Test Suite part 1");

        Set<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(CacheKeepBinaryWithInterceptorTest.class);

        // Atomic caches.
        ignoredTests.add(IgniteCacheEntryListenerAtomicTest.class);
        ignoredTests.add(IgniteCacheEntryListenerAtomicReplicatedTest.class);
        ignoredTests.add(IgniteCacheEntryListenerAtomicLocalTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalExpiryPolicyTest.class);
        ignoredTests.add(IgniteCacheAtomicInvokeTest.class);
        ignoredTests.add(IgniteCacheAtomicNearEnabledInvokeTest.class);
        ignoredTests.add(IgniteCacheAtomicWithStoreInvokeTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalInvokeTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalWithStoreInvokeTest.class);

        ignoredTests.add(GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultiNodeSelfTest.class);
        ignoredTests.add(GridCacheAtomicPartitionedOnlyBinaryDataStreamerMultithreadedSelfTest.class);
        ignoredTests.add(GridCacheAtomicPartitionedOnlyBinaryMultiNodeSelfTest.class);
        ignoredTests.add(GridCacheAtomicPartitionedOnlyBinaryMultithreadedSelfTest.class);

        // Irrelevant tests.
        ignoredTests.add(GridCacheMvccSelfTest.class); // This is about MvccCandidate, but not TxSnapshot.

        // Other non-Tx test.
        ignoredTests.add(GridCacheAffinityRoutingSelfTest.class);
        ignoredTests.add(GridCacheAffinityRoutingBinarySelfTest.class);
        ignoredTests.add(IgniteClientAffinityAssignmentSelfTest.class);

        suite.addTest(IgniteBinaryCacheTestSuite.suite(ignoredTests));

        return suite;
    }
}
