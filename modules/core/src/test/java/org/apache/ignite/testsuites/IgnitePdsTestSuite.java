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
import org.apache.ignite.internal.processors.cache.IgniteClusterActivateDeactivateTestWithPersistence;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDestroyCacheTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDestroyCacheWithoutCheckpointsTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDynamicCacheTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsSingleNodePutGetPersistenceTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsCacheRestoreTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsDataRegionMetricsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.DefaultPageSizeBackwardsCompatibilityTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsCheckpointSimulationWithRealCpDisabledTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsPageReplacementTest;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.IgniteMetaStorageBasicTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.BPlusTreePageMemoryImplTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.BPlusTreeReuseListPageMemoryImplTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.FillFactorMetricTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.IndexStoragePageMemoryImplTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImplNoLoadTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImplTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesWriteThrottleSmokeTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBufferTest;
import org.apache.ignite.internal.processors.database.IgniteDbDynamicCacheSelfTest;
import org.apache.ignite.internal.processors.database.IgniteDbMultiNodePutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbPutGetWithCacheStoreTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodePutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeTinyPutGetTest;

/**
 *
 */
public class IgnitePdsTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Persistent Store Test Suite");

        addRealPageStoreTests(suite);
        addRealPageStoreTestsLongRunning(suite);

        // Basic PageMemory tests.
        suite.addTestSuite(PageMemoryImplNoLoadTest.class);
        suite.addTestSuite(IndexStoragePageMemoryImplTest.class);
        suite.addTestSuite(PageMemoryImplTest.class);

        // BTree tests with store page memory.
        suite.addTestSuite(BPlusTreePageMemoryImplTest.class);
        suite.addTestSuite(BPlusTreeReuseListPageMemoryImplTest.class);

        suite.addTestSuite(SegmentedRingByteBufferTest.class);

        // Write throttling
        suite.addTestSuite(PagesWriteThrottleSmokeTest.class);

        // Metrics
        suite.addTestSuite(FillFactorMetricTest.class);

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to
     * execute.
     *
     * @param suite suite to add tests into.
     */
    public static void addRealPageStoreTestsLongRunning(TestSuite suite) {
        // Basic PageMemory tests.
        suite.addTestSuite(IgnitePdsPageReplacementTest.class);
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store and does actual disk operations.
     *
     * @param suite suite to add tests into.
     */
    public static void addRealPageStoreTests(TestSuite suite) {

        // Checkpointing smoke-test.
        suite.addTestSuite(IgnitePdsCheckpointSimulationWithRealCpDisabledTest.class);

        // Basic API tests.
        suite.addTestSuite(IgniteDbSingleNodePutGetTest.class);
        suite.addTestSuite(IgniteDbMultiNodePutGetTest.class);
        suite.addTestSuite(IgniteDbSingleNodeTinyPutGetTest.class);
        suite.addTestSuite(IgniteDbDynamicCacheSelfTest.class);

        // Persistence-enabled.
        suite.addTestSuite(IgnitePdsSingleNodePutGetPersistenceTest.class);
        suite.addTestSuite(IgnitePdsDynamicCacheTest.class);
        // TODO uncomment when https://issues.apache.org/jira/browse/IGNITE-7510 is fixed
        // suite.addTestSuite(IgnitePdsClientNearCachePutGetTest.class);
        suite.addTestSuite(IgniteDbPutGetWithCacheStoreTest.class);

        suite.addTestSuite(IgniteClusterActivateDeactivateTestWithPersistence.class);

        suite.addTestSuite(IgnitePdsCacheRestoreTest.class);
        suite.addTestSuite(IgnitePdsDataRegionMetricsTest.class);

        suite.addTestSuite(IgnitePdsDestroyCacheTest.class);
        suite.addTestSuite(IgnitePdsDestroyCacheWithoutCheckpointsTest.class);

        suite.addTestSuite(DefaultPageSizeBackwardsCompatibilityTest.class);

        //MetaStorage
        suite.addTestSuite(IgniteMetaStorageBasicTest.class);
    }
}
