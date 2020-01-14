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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedWritesTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreLocalTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStorePartitionedMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStorePartitionedTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreReplicatedTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreSelfTest;
import org.apache.ignite.internal.processors.cache.store.IgnteCacheClientWriteBehindStoreAtomicTest;
import org.apache.ignite.internal.processors.cache.store.IgnteCacheClientWriteBehindStoreNonCoalescingTest;
import org.apache.ignite.internal.processors.cache.store.IgnteCacheClientWriteBehindStoreTxTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite that contains all tests for {@link org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore}.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheWriteBehindTestSuite {
    /**
     * @return Ignite Bamboo in-memory data grid test suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        // Write-behind tests.
        GridTestUtils.addTestIfNeeded(suite, GridCacheWriteBehindStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheWriteBehindStoreMultithreadedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheWriteBehindStoreLocalTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheWriteBehindStoreReplicatedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheWriteBehindStorePartitionedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheWriteBehindStorePartitionedMultiNodeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedWritesTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnteCacheClientWriteBehindStoreAtomicTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnteCacheClientWriteBehindStoreTxTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgnteCacheClientWriteBehindStoreNonCoalescingTest.class, ignoredTests);

        return suite;
    }
}
