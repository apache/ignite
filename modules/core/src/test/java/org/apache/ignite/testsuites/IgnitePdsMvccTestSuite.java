/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheConfigurationFileConsistencyCheckTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheObjectBinaryProcessorOnDiscoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDestroyCacheTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDestroyCacheWithoutCheckpointsTest;
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
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryNoStoreLeakTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesWriteThrottleSmokeTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBufferTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAwareTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsMvccTestSuite {
    /**
     * @return Suite.
     */
    public static List<Class<?>> suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        Set<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests.
        ignoredTests.add(IgnitePdsCheckpointSimulationWithRealCpDisabledTest.class);

        // Atomic tests.
        ignoredTests.add(IgnitePdsDataRegionMetricsTest.class);

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

        return new ArrayList<>(IgnitePdsTestSuite.suite(ignoredTests));
    }
}
