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
import org.apache.ignite.internal.processors.cache.CacheMetricsEnableRuntimeTest;
import org.apache.ignite.internal.processors.cache.CacheMetricsEntitiesCountTest;
import org.apache.ignite.internal.processors.cache.CacheMetricsForClusterGroupSelfTest;
import org.apache.ignite.internal.processors.cache.CacheValidatorMetricsTest;
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
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * Test suite for cache metrics.
 */
public class IgniteCacheMetricsSelfTestWithPersistenceSuite extends TestSuite {
    /**
     * @return Cache metrics test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache Metrics Test Suite");

        System.setProperty(GridAbstractTest.PERSISTENCE_IN_TESTS_IS_ALLOWED_PROPERTY, "true");

        suite.addTestSuite(CacheGroupMetricsMBeanTest.class);
        suite.addTestSuite(CacheMetricsEnableRuntimeTest.class);

        return suite;
    }
}
