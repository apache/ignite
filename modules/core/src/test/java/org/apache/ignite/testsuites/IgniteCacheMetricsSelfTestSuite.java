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
import org.apache.ignite.internal.TransactionMetricsTest;
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

/**
 * Test suite for cache metrics.
 */
public class IgniteCacheMetricsSelfTestSuite {
    /**
     * @param ignoredTests Ignored tests.
     * @return Cache metrics test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

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

        GridTestUtils.addTestIfNeeded(suite, TransactionMetricsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridEvictionPolicyMBeansTest.class, ignoredTests);

        return suite;
    }
}
