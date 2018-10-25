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
import junit.framework.TestSuite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicMessageCountSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteAtomicCacheEntryProcessorNodeJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtAtomicEvictionNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEvictionEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearReadersSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAtomicGetAndTransformStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicBasicStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicGetAndTransformStoreSelfTest;

/**
 * Test suite.
 */
public class IgniteCacheMvccTestSuite2 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     */
    public static TestSuite suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        HashSet<Class> ignoredTests = new HashSet<>();
        ignoredTests.add(GridCacheLocalAtomicBasicStoreSelfTest.class);
        ignoredTests.add(GridCacheLocalAtomicGetAndTransformStoreSelfTest.class);
        ignoredTests.add(GridCacheAtomicNearMultiNodeSelfTest.class);
        ignoredTests.add(GridCacheAtomicNearReadersSelfTest.class);
        ignoredTests.add(GridCachePartitionedAtomicGetAndTransformStoreSelfTest.class);
        ignoredTests.add(GridCacheAtomicNearEvictionEventSelfTest.class);
        ignoredTests.add(GridCacheAtomicMessageCountSelfTest.class);
        ignoredTests.add(IgniteAtomicCacheEntryProcessorNodeJoinTest.class);
        ignoredTests.add(GridCacheDhtAtomicEvictionNearReadersSelfTest.class);

        TestSuite suite = new TestSuite("IgniteCache Mvcc Test Suite part 2");

        suite.addTest(IgniteCacheTestSuite2.suite(ignoredTests));

        return suite;
    }
}
