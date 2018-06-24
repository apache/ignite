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
import org.apache.ignite.internal.processors.authentication.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingPartitionCountersTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingWithAsyncClearingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCacheAssignmentNodeRestartsTest;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * Test suite.
 */
public class IgniteCacheTestSuiteWithPersistent extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("IgniteCache Test Suite With Persistence");

        System.setProperty(GridAbstractTest.PERSISTENCE_IN_TESTS_IS_ALLOWED_PROPERTY, "true");

        suite.addTestSuite(AuthenticationConfigurationClusterTest.class);
        suite.addTestSuite(AuthenticationProcessorSelfTest.class);
        suite.addTestSuite(AuthenticationOnNotActiveClusterTest.class);
        suite.addTestSuite(AuthenticationProcessorNodeRestartTest.class);
        suite.addTestSuite(AuthenticationProcessorNPEOnStartTest.class);
        suite.addTestSuite(Authentication1kUsersNodeRestartTest.class);

        suite.addTestSuite(CacheDataRegionConfigurationTest.class);

        suite.addTestSuite(WalModeChangeAdvancedSelfTest.class);
        suite.addTestSuite(WalModeChangeSelfTest.class);
        suite.addTestSuite(WalModeChangeCoordinatorNotAffinityNodeSelfTest.class);

        suite.addTestSuite(Cache64kPartitionsTest.class);
        suite.addTestSuite(GridCacheRebalancingPartitionCountersTest.class);
        suite.addTestSuite(GridCacheRebalancingWithAsyncClearingTest.class);

        suite.addTestSuite(IgnitePdsCacheAssignmentNodeRestartsTest.class);
        suite.addTestSuite(TxRollbackAsyncTest.class);

        return suite;
    }
}
