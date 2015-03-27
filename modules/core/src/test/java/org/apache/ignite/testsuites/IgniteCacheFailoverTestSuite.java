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
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;

/**
 * Test suite.
 */
public class IgniteCacheFailoverTestSuite extends TestSuite {
    /**
     * @return Ignite Cache Group Lock Failover test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache Failover Test Suite");

        suite.addTestSuite(GridCacheAtomicInvalidPartitionHandlingSelfTest.class);

        // Group lock failover.
        // TODO: IGNITE-80.
        //suite.addTestSuite(GridCacheGroupLockFailoverSelfTest.class);
        //suite.addTestSuite(GridCacheGroupLockFailoverOptimisticTxSelfTest.class);

        suite.addTestSuite(GridCacheIncrementTransformTest.class);

        // Failure consistency tests.
        suite.addTestSuite(GridCacheAtomicRemoveFailureTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderRemoveFailureTest.class);

        suite.addTestSuite(GridCacheDhtAtomicRemoveFailureTest.class);
        suite.addTestSuite(GridCacheDhtRemoveFailureTest.class);
        suite.addTestSuite(GridCacheNearRemoveFailureTest.class);
        //suite.addTestSuite(GridCacheAtomicNearRemoveFailureTest.class); TODO IGNITE-560
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderNearRemoveFailureTest.class);

        //suite.addTest(new TestSuite(GridCachePartitionedFailoverSelfTest.class));  TODO-gg-4813
        //suite.addTest(new TestSuite(GridCacheColocatedFailoverSelfTest.class)); TODO-gg-4813
        //suite.addTestSuite(GridCacheReplicatedFailoverSelfTest.class); TODO-gg-4813

        return suite;
    }
}
