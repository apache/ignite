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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.IgniteClusterActivateDeactivateTestWithPersistence;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheConfigurationFileConsistencyCheckTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheObjectBinaryProcessorOnDiscoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDestroyCacheTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDestroyCacheWithoutCheckpointsTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDynamicCacheTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsSingleNodePutGetPersistenceTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsCacheRestoreTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsDataRegionMetricsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsWithTtlTest;
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
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryNoStoreLeakTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesWriteThrottleSmokeTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.CpTriggeredWalDeltaConsistencyTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.ExplicitWalDeltaConsistencyTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBufferTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.SysPropWalDeltaConsistencyTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAwareTest;
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
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Persistent Store Test Suite");

        addRealPageStoreTests(suite);
        addRealPageStoreTestsLongRunning(suite);

        // Basic PageMemory tests.
        //suite.addTest(new JUnit4TestAdapter(PageMemoryNoLoadSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(PageMemoryImplNoLoadTest.class));
        suite.addTest(new JUnit4TestAdapter(PageMemoryNoStoreLeakTest.class));
        suite.addTest(new JUnit4TestAdapter(IndexStoragePageMemoryImplTest.class));
        suite.addTest(new JUnit4TestAdapter(PageMemoryImplTest.class));
        //suite.addTest(new JUnit4TestAdapter(PageIdDistributionTest.class));
        //suite.addTest(new JUnit4TestAdapter(TrackingPageIOTest.class));

        // BTree tests with store page memory.
        suite.addTest(new JUnit4TestAdapter(BPlusTreePageMemoryImplTest.class));
        suite.addTest(new JUnit4TestAdapter(BPlusTreeReuseListPageMemoryImplTest.class));

        suite.addTest(new JUnit4TestAdapter(SegmentedRingByteBufferTest.class));

        // Write throttling
        suite.addTest(new JUnit4TestAdapter(PagesWriteThrottleSmokeTest.class));

        // Metrics
        suite.addTest(new JUnit4TestAdapter(FillFactorMetricTest.class));

        // WAL delta consistency
        suite.addTest(new JUnit4TestAdapter(CpTriggeredWalDeltaConsistencyTest.class));
        suite.addTest(new JUnit4TestAdapter(ExplicitWalDeltaConsistencyTest.class));
        suite.addTest(new JUnit4TestAdapter(SysPropWalDeltaConsistencyTest.class));

        // Binary meta tests.
        suite.addTest(new JUnit4TestAdapter(IgnitePdsCacheObjectBinaryProcessorOnDiscoveryTest.class));

        suite.addTest(new JUnit4TestAdapter(SegmentAwareTest.class));

        return suite;
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store, but requires long time to
     * execute.
     *
     * @param suite suite to add tests into.
     */
    private static void addRealPageStoreTestsLongRunning(TestSuite suite) {
        // Basic PageMemory tests.
        suite.addTest(new JUnit4TestAdapter(IgnitePdsPageReplacementTest.class));
    }

    /**
     * Fills {@code suite} with PDS test subset, which operates with real page store and does actual disk operations.
     *
     * NOTE: These tests are also executed using I/O plugins.
     *
     * @param suite suite to add tests into.
     */
    public static void addRealPageStoreTests(TestSuite suite) {

        // Checkpointing smoke-test.
        suite.addTest(new JUnit4TestAdapter(IgnitePdsCheckpointSimulationWithRealCpDisabledTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgnitePdsCheckpointSimpleTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgnitePersistenceSequentialCheckpointTest.class));

        // Basic API tests.
        suite.addTest(new JUnit4TestAdapter(IgniteDbSingleNodePutGetTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDbMultiNodePutGetTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDbSingleNodeTinyPutGetTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDbDynamicCacheSelfTest.class));

        // Persistence-enabled.
        suite.addTest(new JUnit4TestAdapter(IgnitePdsSingleNodePutGetPersistenceTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsDynamicCacheTest.class));
        // TODO uncomment when https://issues.apache.org/jira/browse/IGNITE-7510 is fixed
        // suite.addTest(new JUnit4TestAdapter(IgnitePdsClientNearCachePutGetTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDbPutGetWithCacheStoreTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsWithTtlTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteClusterActivateDeactivateTestWithPersistence.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsCacheRestoreTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsDataRegionMetricsTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsDestroyCacheTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgnitePdsRemoveDuringRebalancingTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsDestroyCacheWithoutCheckpointsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsCacheConfigurationFileConsistencyCheckTest.class));

        suite.addTest(new JUnit4TestAdapter(DefaultPageSizeBackwardsCompatibilityTest.class));

        //MetaStorage
        suite.addTest(new JUnit4TestAdapter(IgniteMetaStorageBasicTest.class));
    }
}
