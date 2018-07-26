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
import org.apache.ignite.internal.TransactionMetricsMxBeanImplTest;
import org.apache.ignite.internal.processors.cache.CacheGroupMetricsMBeanTest;
import org.apache.ignite.internal.processors.cache.CacheGroupsMetricsRebalanceTest;
import org.apache.ignite.internal.processors.cache.CacheMetricsManageTest;
import org.apache.ignite.internal.processors.cache.CacheMetricsEntitiesCountTest;
import org.apache.ignite.internal.processors.cache.CacheMetricsForClusterGroupSelfTest;
import org.apache.ignite.internal.processors.cache.CacheValidatorMetricsTest;
import org.apache.ignite.internal.processors.cache.GridEvictionPolicyMBeansTest;
import org.apache.ignite.internal.processors.cache.OffheapCacheMetricsForClusterGroupSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPartitionedMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPartitionedTckMetricsSelfTestImpl;
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

/**
 * Test suite for cache metrics.
 */
public class IgniteCacheMetricsSelfTestSuite extends TestSuite {
    /**
     * @return Cache metrics test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache Metrics Test Suite");

        suite.addTestSuite(GridCacheLocalMetricsSelfTest.class);
        suite.addTestSuite(GridCacheLocalAtomicMetricsNoReadThroughSelfTest.class);
        suite.addTestSuite(GridCacheNearMetricsSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedMetricsSelfTest.class);
        suite.addTestSuite(GridCachePartitionedMetricsSelfTest.class);
        suite.addTestSuite(GridCachePartitionedHitsAndMissesSelfTest.class);

        // Atomic cache.
        suite.addTestSuite(GridCacheAtomicLocalMetricsSelfTest.class);
        suite.addTestSuite(GridCacheAtomicLocalMetricsNoStoreSelfTest.class);
        suite.addTestSuite(GridCacheAtomicReplicatedMetricsSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPartitionedMetricsSelfTest.class);
        suite.addTestSuite(GridCacheAtomicPartitionedTckMetricsSelfTestImpl.class);
        suite.addTestSuite(GridCacheAtomicLocalTckMetricsSelfTestImpl.class);

        suite.addTestSuite(CacheGroupsMetricsRebalanceTest.class);
        suite.addTestSuite(CacheGroupMetricsMBeanTest.class);
        suite.addTestSuite(CacheValidatorMetricsTest.class);
        suite.addTestSuite(CacheMetricsManageTest.class);
        suite.addTestSuite(CacheMetricsEntitiesCountTest.class);

        // Cluster wide metrics.
        suite.addTestSuite(CacheMetricsForClusterGroupSelfTest.class);
        suite.addTestSuite(OffheapCacheMetricsForClusterGroupSelfTest.class);

        suite.addTestSuite(TransactionMetricsMxBeanImplTest.class);

        suite.addTestSuite(GridEvictionPolicyMBeansTest.class);

        return suite;
    }
}
