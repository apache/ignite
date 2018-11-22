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
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheVersionMultinodeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicNearPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicReplicatedPeekModesTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLoadAllTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLoaderWriterTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLocalLoadAllTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLocalNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLocalNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicLocalNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNearEnabledNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNearEnabledNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNearEnabledNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNoLoadPreviousValueTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNoReadThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicNoWriteThroughTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicStoreSessionTest;
import org.apache.ignite.internal.processors.cache.integration.IgniteCacheAtomicStoreSessionWriteBehindTest;

/**
 *
 */
public class IgniteCacheMvccTestSuite4 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     */
    public static TestSuite suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        HashSet<Class> ignoredTests = new HashSet<>(128);

        // Skip classes that already contains Mvcc tests
        // ignoredTests.add(GridCacheTransformEventSelfTest.class);
        ignoredTests.add(GridCacheVersionMultinodeTest.class);

        // Optimistic tx tests.
        // ignoredTests.add(GridCacheColocatedOptimisticTransactionSelfTest.class);

        // Irrelevant Tx tests.
        // ignoredTests.add(IgniteOnePhaseCommitInvokeTest.class);

        // Atomic cache tests.
        // ignoredTests.add(GridCacheLocalAtomicBasicStoreSelfTest.class);
        ignoredTests.add(GridCacheMultinodeUpdateAtomicSelfTest.class);
        ignoredTests.add(GridCacheMultinodeUpdateAtomicNearEnabledSelfTest.class);
        ignoredTests.add(IgniteCacheAtomicLoadAllTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalLoadAllTest.class);
        ignoredTests.add(IgniteCacheAtomicLoaderWriterTest.class);
        ignoredTests.add(IgniteCacheAtomicStoreSessionTest.class);
        ignoredTests.add(IgniteCacheAtomicStoreSessionWriteBehindTest.class);
        ignoredTests.add(IgniteCacheAtomicNoReadThroughTest.class);
        ignoredTests.add(IgniteCacheAtomicNearEnabledNoReadThroughTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalNoReadThroughTest.class);
        ignoredTests.add(IgniteCacheAtomicNoLoadPreviousValueTest.class);
        ignoredTests.add(IgniteCacheAtomicNearEnabledNoLoadPreviousValueTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalNoLoadPreviousValueTest.class);
        ignoredTests.add(IgniteCacheAtomicNoWriteThroughTest.class);
        ignoredTests.add(IgniteCacheAtomicNearEnabledNoWriteThroughTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalNoWriteThroughTest.class);
        ignoredTests.add(IgniteCacheAtomicPeekModesTest.class);
        ignoredTests.add(IgniteCacheAtomicNearPeekModesTest.class);
        ignoredTests.add(IgniteCacheAtomicReplicatedPeekModesTest.class);
        ignoredTests.add(IgniteCacheAtomicLocalPeekModesTest.class);




        // Other non-tx tests.
        // ignoredTests.add(RendezvousAffinityFunctionSelfTest.class);

        // Skip classes which Mvcc implementations are added in this method below.
        // TODO IGNITE-10175: refactor these tests (use assume) to support both mvcc and non-mvcc modes after moving to JUnit4/5.
        // ignoredTests.add(GridCachePartitionedTxSingleThreadedSelfTest.class); // See GridCachePartitionedMvccTxSingleThreadedSelfTest



        TestSuite suite = new TestSuite("IgniteCache Mvcc Test Suite part 4");

        // TODO UNCOMMENT!! suite.addTest(IgniteCacheTestSuite4.suite(ignoredTests));
        suite.addTestSuite(GridCacheVersionMultinodeTest.class);

        // Add Mvcc clones.
        // suite.addTestSuite(GridCachePartitionedMvccTxSingleThreadedSelfTest.class);

        return suite;
    }
}
