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

import junit.framework.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.testframework.*;

import java.util.*;

/**
 * Test suite.
 */
public class IgniteCacheFailoverTestSuite extends TestSuite {
    /**
     * @return Ignite Cache Failover test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution.
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite(Set<Class> ignoredTests) throws Exception {
        TestSuite suite = new TestSuite("Cache Failover Test Suite");

        suite.addTestSuite(GridCacheAtomicInvalidPartitionHandlingSelfTest.class);
        suite.addTestSuite(GridCacheAtomicClientInvalidPartitionHandlingSelfTest.class);

        GridTestUtils.addTestIfNeeded(suite, GridCacheIncrementTransformTest.class, ignoredTests);

        // Failure consistency tests.
        suite.addTestSuite(GridCacheAtomicRemoveFailureTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderRemoveFailureTest.class);
        suite.addTestSuite(GridCacheAtomicClientRemoveFailureTest.class);

        suite.addTestSuite(GridCacheDhtAtomicRemoveFailureTest.class);
        suite.addTestSuite(GridCacheDhtRemoveFailureTest.class);
        suite.addTestSuite(GridCacheDhtClientRemoveFailureTest.class);
        suite.addTestSuite(GridCacheNearRemoveFailureTest.class);
        suite.addTestSuite(GridCacheAtomicNearRemoveFailureTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderNearRemoveFailureTest.class);

        suite.addTestSuite(IgniteCacheAtomicNodeJoinTest.class);
        suite.addTestSuite(IgniteCacheTxNodeJoinTest.class);
        suite.addTestSuite(IgniteCacheTxFairAffinityNodeJoinTest.class);

        suite.addTestSuite(IgniteCacheTxNearDisabledPutGetRestartTest.class);
        suite.addTestSuite(IgniteCacheTxNearDisabledFairAffinityPutGetRestartTest.class);

        suite.addTestSuite(IgniteCachePutRetryAtomicSelfTest.class);
        suite.addTestSuite(IgniteCachePutRetryAtomicPrimaryWriteOrderSelfTest.class);
        suite.addTestSuite(IgniteCachePutRetryTransactionalSelfTest.class);

        suite.addTestSuite(IgniteCacheSslStartStopSelfTest.class);

        suite.addTestSuite(IgniteCacheSizeFailoverTest.class);

        suite.addTestSuite(IgniteCacheTopologySafeGetSelfTest.class);

        return suite;
    }
}
