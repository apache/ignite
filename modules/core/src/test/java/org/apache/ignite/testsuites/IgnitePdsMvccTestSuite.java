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
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheConfigurationFileConsistencyCheckTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheObjectBinaryProcessorOnDiscoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDestroyCacheTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDestroyCacheWithoutCheckpointsTest;
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
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBufferTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAwareTest;

/**
 *
 */
public class IgnitePdsMvccTestSuite extends TestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        TestSuite suite = new TestSuite("Ignite Persistent Store Mvcc Test Suite");

        Set<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests.
        ignoredTests.add(IgnitePdsCheckpointSimulationWithRealCpDisabledTest.class);

        // Non-relevant tests.
        ignoredTests.add(IgnitePdsCacheConfigurationFileConsistencyCheckTest.class);
        ignoredTests.add(DefaultPageSizeBackwardsCompatibilityTest.class);
        ignoredTests.add(IgniteMetaStorageBasicTest.class);

        ignoredTests.add(IgnitePdsPageReplacementTest.class);

        ignoredTests.add(PageMemoryImplNoLoadTest.class);
        ignoredTests.add(PageMemoryNoStoreLeakTest.class);
        ignoredTests.add(IndexStoragePageMemoryImplTest.class);
        ignoredTests.add(PageMemoryImplTest.class);
        ignoredTests.add(BPlusTreePageMemoryImplTest.class);
        ignoredTests.add(BPlusTreeReuseListPageMemoryImplTest.class);
        ignoredTests.add(SegmentedRingByteBufferTest.class);
        ignoredTests.add(PagesWriteThrottleSmokeTest.class);
        ignoredTests.add(FillFactorMetricTest.class);
        ignoredTests.add(IgnitePdsCacheObjectBinaryProcessorOnDiscoveryTest.class);
        ignoredTests.add(SegmentAwareTest.class);

        ignoredTests.add(IgnitePdsDestroyCacheTest.class);
        ignoredTests.add(IgnitePdsDestroyCacheWithoutCheckpointsTest.class);

        suite.addTest(IgnitePdsTestSuite.suite(ignoredTests));

        return suite;
    }
}
