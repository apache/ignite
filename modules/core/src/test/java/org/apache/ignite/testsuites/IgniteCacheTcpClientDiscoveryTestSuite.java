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
import org.apache.ignite.internal.processors.cache.GridCacheTcpClientDiscoveryMultiThreadedTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

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
@RunWith(DynamicSuite.class)
public class IgniteCacheTcpClientDiscoveryTestSuite {
    /**
     * @return Suite.
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
