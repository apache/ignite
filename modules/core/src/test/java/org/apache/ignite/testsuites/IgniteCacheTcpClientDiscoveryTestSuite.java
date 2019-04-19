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
import org.apache.ignite.internal.processors.cache.GridCacheTcpClientDiscoveryMultiThreadedTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import static org.apache.ignite.internal.processors.cache.distributed.GridCacheClientModesTcpClientDiscoveryAbstractTest.CaseClientPartitionedAtomic;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheClientModesTcpClientDiscoveryAbstractTest.CaseClientPartitionedTransactional;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheClientModesTcpClientDiscoveryAbstractTest.CaseClientReplicatedAtomic;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheClientModesTcpClientDiscoveryAbstractTest.CaseClientReplicatedTransactional;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheClientModesTcpClientDiscoveryAbstractTest.CaseNearPartitionedAtomic;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheClientModesTcpClientDiscoveryAbstractTest.CaseNearPartitionedTransactional;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheClientModesTcpClientDiscoveryAbstractTest.CaseNearReplicatedAtomic;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheClientModesTcpClientDiscoveryAbstractTest.CaseNearReplicatedTransactional;

/**
 * Tests a cache with TcpClientDiscovery SPI being enabled.
 */
@RunWith(AllTests.class)
public class IgniteCacheTcpClientDiscoveryTestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static TestSuite suite(Collection<Class> ignoredTests) {
        TestSuite suite = new TestSuite("Cache + TcpClientDiscovery SPI test suite.");

        GridTestUtils.addTestIfNeeded(suite, CaseNearPartitionedAtomic.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CaseNearPartitionedTransactional.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CaseNearReplicatedAtomic.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CaseNearReplicatedTransactional.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CaseClientPartitionedAtomic.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CaseClientPartitionedTransactional.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CaseClientReplicatedAtomic.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CaseClientReplicatedTransactional.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheTcpClientDiscoveryMultiThreadedTest.class, ignoredTests);

        return suite;
    }
}
