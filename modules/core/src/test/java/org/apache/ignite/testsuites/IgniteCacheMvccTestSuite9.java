/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.IgniteCacheGetCustomCollectionsSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLoadRebalanceEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAtomicPrimarySyncBackPressureTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxConcurrentRemoveObjectsTest;
import org.apache.ignite.internal.stat.IoStatisticsCachePersistenceSelfTest;
import org.apache.ignite.internal.stat.IoStatisticsCacheSelfTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheMvccTestSuite9 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        Collection<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(IgniteTxConcurrentRemoveObjectsTest.class);

        // Non supported modes.
        ignoredTests.add(IgniteCachePrimarySyncTest.class);
        ignoredTests.add(IgniteTxCachePrimarySyncTest.class);

        // Atomic caches.
        ignoredTests.add(CacheAtomicPrimarySyncBackPressureTest.class);

        // Other non-tx tests.
        ignoredTests.add(IgniteCacheGetCustomCollectionsSelfTest.class);
        ignoredTests.add(IgniteCacheLoadRebalanceEvictionSelfTest.class);

        // IO statistics.
        ignoredTests.add(IoStatisticsCacheSelfTest.class);
        ignoredTests.add(IoStatisticsCachePersistenceSelfTest.class);

        return new ArrayList<>(IgniteCacheTestSuite9.suite(ignoredTests));
    }
}
