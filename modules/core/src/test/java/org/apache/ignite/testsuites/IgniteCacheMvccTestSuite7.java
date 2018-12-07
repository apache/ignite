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
import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.authentication.Authentication1kUsersNodeRestartTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationConfigurationClusterTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationOnNotActiveClusterTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationProcessorNPEOnStartTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationProcessorNodeRestartTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest;
import org.apache.ignite.internal.processors.cache.CacheDataRegionConfigurationTest;
import org.apache.ignite.internal.processors.cache.CacheGroupMetricsMBeanTest;
import org.apache.ignite.internal.processors.cache.MvccCacheGroupMetricsMBeanTest;
import org.apache.ignite.internal.processors.cache.distributed.Cache64kPartitionsTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingPartitionCountersMvccTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingPartitionCountersTest;
import org.apache.ignite.internal.processors.cache.persistence.db.CheckpointBufferDeadlockTest;

/**
 *
 */
public class IgniteCacheMvccTestSuite7  extends TestSuite {
    /**
     * @return IgniteCache test suite.
     */
    public static TestSuite suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        HashSet<Class> ignoredTests = new HashSet<>(128);

        // Other non-tx tests.
        ignoredTests.add(CheckpointBufferDeadlockTest.class);//
        ignoredTests.add(AuthenticationConfigurationClusterTest.class);//
        ignoredTests.add(AuthenticationProcessorSelfTest.class);
        ignoredTests.add(AuthenticationOnNotActiveClusterTest.class);
        ignoredTests.add(AuthenticationProcessorNodeRestartTest.class);
        ignoredTests.add(AuthenticationProcessorNPEOnStartTest.class);
        ignoredTests.add(Authentication1kUsersNodeRestartTest.class);
        ignoredTests.add(CacheDataRegionConfigurationTest.class);
        ignoredTests.add(Cache64kPartitionsTest.class);

        // Skip classes which Mvcc implementations are added in this method below.
        // TODO IGNITE-10175: refactor these tests (use assume) to support both mvcc and non-mvcc modes after moving to JUnit4/5.
        ignoredTests.add(CacheGroupMetricsMBeanTest.class); // See MvccCacheGroupMetricsMBeanTest
        ignoredTests.add(GridCacheRebalancingPartitionCountersTest.class); // See GridCacheRebalancingPartitionCountersMvccTest

        TestSuite suite = new TestSuite("IgniteCache Mvcc Test Suite part 7");

        suite.addTest(IgniteCacheTestSuite7.suite(ignoredTests));

        // Add Mvcc clones.
        suite.addTest(new JUnit4TestAdapter(MvccCacheGroupMetricsMBeanTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheRebalancingPartitionCountersMvccTest.class));


        return suite;
    }

}
