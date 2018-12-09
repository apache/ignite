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
import org.apache.ignite.internal.TransactionMetricsMxBeanImplTest;
import org.apache.ignite.internal.processors.cache.CacheGroupsMetricsRebalanceTest;
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
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite for cache metrics.
 */
@RunWith(AllTests.class)
public class IgniteCacheMetricsSelfTestSuite {
    /**
     * @return Cache metrics test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Cache Metrics Test Suite");

        suite.addTest(new JUnit4TestAdapter(GridCacheLocalMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheLocalAtomicMetricsNoReadThroughSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheNearMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheNearAtomicMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedHitsAndMissesSelfTest.class));

        // Atomic cache.
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicLocalMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicLocalMetricsNoStoreSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicReplicatedMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicPartitionedMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicPartitionedTckMetricsSelfTestImpl.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicLocalTckMetricsSelfTestImpl.class));

        suite.addTest(new JUnit4TestAdapter(CacheGroupsMetricsRebalanceTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheValidatorMetricsTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMetricsEntitiesCountTest.class));

        // Cluster wide metrics.
        suite.addTest(new JUnit4TestAdapter(CacheMetricsForClusterGroupSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(OffheapCacheMetricsForClusterGroupSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(TransactionMetricsMxBeanImplTest.class));

        suite.addTest(new JUnit4TestAdapter(GridEvictionPolicyMBeansTest.class));

        return suite;
    }
}
