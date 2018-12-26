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

import java.util.Set;
import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.authentication.Authentication1kUsersNodeRestartTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationConfigurationClusterTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationOnNotActiveClusterTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationProcessorNPEOnStartTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationProcessorNodeRestartTest;
import org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest;
import org.apache.ignite.internal.processors.cache.CacheDataRegionConfigurationTest;
import org.apache.ignite.internal.processors.cache.CacheGroupMetricsMBeanTest;
import org.apache.ignite.internal.processors.cache.CacheMetricsManageTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicCacheStartFailWithPersistenceTest;
import org.apache.ignite.internal.processors.cache.WalModeChangeAdvancedSelfTest;
import org.apache.ignite.internal.processors.cache.WalModeChangeCoordinatorNotAffinityNodeSelfTest;
import org.apache.ignite.internal.processors.cache.WalModeChangeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.Cache64kPartitionsTest;
import org.apache.ignite.internal.processors.cache.distributed.CachePageWriteLockUnlockTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheRentingStateRepairTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheDataLossOnPartitionMoveTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheStartWithLoadTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingPartitionCountersTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingWithAsyncClearingTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionMultinodeMixedRegionsTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheAssignmentNodeRestartsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.CheckpointBufferDeadlockTest;
import org.apache.ignite.internal.processors.cache.transactions.TransactionIntegrityWithPrimaryIndexCorruptionTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackAsyncWithPersistenceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxWithSmallTimeoutAndContentionOneKeyTest;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite7 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     */
    public static TestSuite suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static TestSuite suite(Set<Class> ignoredTests) {
        TestSuite suite = new TestSuite("IgniteCache With Persistence Test Suite");

        suite.addTest(new JUnit4TestAdapter(CheckpointBufferDeadlockTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheStartWithLoadTest.class));

        suite.addTest(new JUnit4TestAdapter(AuthenticationConfigurationClusterTest.class));
        suite.addTest(new JUnit4TestAdapter(AuthenticationProcessorSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(AuthenticationOnNotActiveClusterTest.class));
        suite.addTest(new JUnit4TestAdapter(AuthenticationProcessorNodeRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(AuthenticationProcessorNPEOnStartTest.class));
        suite.addTest(new JUnit4TestAdapter(Authentication1kUsersNodeRestartTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheDataRegionConfigurationTest.class));

        suite.addTest(new JUnit4TestAdapter(WalModeChangeAdvancedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(WalModeChangeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(WalModeChangeCoordinatorNotAffinityNodeSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(Cache64kPartitionsTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheRebalancingPartitionCountersTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheRebalancingWithAsyncClearingTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsCacheAssignmentNodeRestartsTest.class));
        suite.addTest(new JUnit4TestAdapter(TxRollbackAsyncWithPersistenceTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheGroupMetricsMBeanTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMetricsManageTest.class));
        suite.addTest(new JUnit4TestAdapter(PageEvictionMultinodeMixedRegionsTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteDynamicCacheStartFailWithPersistenceTest.class));

        suite.addTest(new JUnit4TestAdapter(TxWithSmallTimeoutAndContentionOneKeyTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheRentingStateRepairTest.class));

        suite.addTest(new JUnit4TestAdapter(TransactionIntegrityWithPrimaryIndexCorruptionTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheDataLossOnPartitionMoveTest.class));

        suite.addTest(new JUnit4TestAdapter(CachePageWriteLockUnlockTest.class));

        return suite;
    }
}
