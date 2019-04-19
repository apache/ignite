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

import java.util.Collection;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.CachePutIfAbsentTest;
import org.apache.ignite.internal.processors.cache.GridCacheLongRunningTransactionDiagnosticsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheGetCustomCollectionsSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLoadRebalanceEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAtomicPrimarySyncBackPressureTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheOperationsInterruptTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCacheWriteSynchronizationModesMultithreadedTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxConcurrentRemoveObjectsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxDataConsistencyOnCommitFailureTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite.
 */
@RunWith(AllTests.class)
public class IgniteCacheTestSuite9 {
    /**
     * @return IgniteCache test suite.
     */
    public static TestSuite suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Ignored tests.
     * @return IgniteCache test suite.
     */
    public static TestSuite suite(Collection<Class> ignoredTests) {
        TestSuite suite = new TestSuite("IgniteCache Test Suite part 9");

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheGetCustomCollectionsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheLoadRebalanceEvictionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePrimarySyncTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTxCachePrimarySyncTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTxCacheWriteSynchronizationModesMultithreadedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CachePutIfAbsentTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheAtomicPrimarySyncBackPressureTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteTxConcurrentRemoveObjectsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TxDataConsistencyOnCommitFailureTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheOperationsInterruptTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheLongRunningTransactionDiagnosticsTest.class, ignoredTests);

        return suite;
    }
}
