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

package org.apache.ignite;

import java.util.Collection;
import java.util.HashSet;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicNodeJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtAtomicRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheTxNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.AtomicPutAllChangingTopologyTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicClientInvalidPartitionHandlingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicClientRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicInvalidPartitionHandlingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearRemoveFailureTest;
import org.apache.ignite.testsuites.IgniteCacheFailoverTestSuite;

/**
 * Ignite mvcc cache failover test suite.
 */
public class IgniteMvccCacheFailoverTestSuite extends TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        Collection<Class> ignoredTests = new HashSet<>();

        // Skip classes that are already has mvcc tests.
        ignoredTests.add(GridCacheTxNodeFailureSelfTest.class);

        // Skip atomic cache tests.
        ignoredTests.add(GridCacheAtomicInvalidPartitionHandlingSelfTest.class);
        ignoredTests.add(GridCacheAtomicClientInvalidPartitionHandlingSelfTest.class);
        ignoredTests.add(GridCacheAtomicRemoveFailureTest.class);
        ignoredTests.add(GridCacheAtomicClientRemoveFailureTest.class);
        ignoredTests.add(GridCacheDhtAtomicRemoveFailureTest.class);
        ignoredTests.add(GridCacheAtomicNearRemoveFailureTest.class);
        ignoredTests.add(IgniteCacheAtomicNodeJoinTest.class);
        ignoredTests.add(AtomicPutAllChangingTopologyTest.class);

        //TODO IGNITE-10458: ignore or fix? IgniteAtomicLongChangingTopologySelfTest.

        TestSuite suite = new TestSuite("Mvcc Cache Failover Test Suite");

        suite.addTest(IgniteCacheFailoverTestSuite.suite(ignoredTests));

        return suite;
    }
}
