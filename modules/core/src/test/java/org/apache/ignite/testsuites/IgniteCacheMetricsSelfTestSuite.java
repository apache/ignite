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

import java.util.Collection;
import junit.framework.TestSuite;
import org.apache.ignite.internal.TransactionMetricsMxBeanImplTest;
import org.apache.ignite.internal.processors.cache.CacheGroupsMetricsRebalanceTest;
import org.apache.ignite.internal.processors.cache.CacheMetricsCacheSizeTest;
import org.apache.ignite.internal.processors.cache.CacheMetricsEntitiesCountTest;
import org.apache.ignite.internal.processors.cache.CacheMetricsForClusterGroupSelfTest;
import org.apache.ignite.internal.processors.cache.CacheValidatorMetricsTest;
import org.apache.ignite.internal.processors.cache.GridEvictionPolicyMBeansTest;
import org.apache.ignite.internal.processors.cache.OffheapCacheMetricsForClusterGroupSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPartitionedMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPartitionedTckMetricsSelfTestImpl;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearAtomicMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedHitsAndMissesSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheAtomicReplicatedMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheAtomicLocalMetricsNoStoreSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheAtomicLocalMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheAtomicLocalTckMetricsSelfTestImpl;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicMetricsNoReadThroughSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalMetricsSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite for cache metrics.
 */
@RunWith(AllTests.class)
public class IgniteCacheMetricsSelfTestSuite {
    /**
     * @param ignoredTests Ignored tests.
     * @return Cache metrics test suite.
     */
    public static TestSuite suite(Collection<Class> ignoredTests) {
        TestSuite suite = new TestSuite("Cache Metrics Test Suite");

        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalMetricsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheLocalAtomicMetricsNoReadThroughSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearMetricsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheNearAtomicMetricsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheReplicatedMetricsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedMetricsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCachePartitionedHitsAndMissesSelfTest.class, ignoredTests);

        // Atomic cache.
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicLocalMetricsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicLocalMetricsNoStoreSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicReplicatedMetricsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicPartitionedMetricsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicPartitionedTckMetricsSelfTestImpl.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicLocalTckMetricsSelfTestImpl.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheGroupsMetricsRebalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheValidatorMetricsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheMetricsEntitiesCountTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheMetricsCacheSizeTest.class, ignoredTests);

        // Cluster wide metrics.
        GridTestUtils.addTestIfNeeded(suite, CacheMetricsForClusterGroupSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, OffheapCacheMetricsForClusterGroupSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TransactionMetricsMxBeanImplTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridEvictionPolicyMBeansTest.class, ignoredTests);

        return suite;
    }
}
