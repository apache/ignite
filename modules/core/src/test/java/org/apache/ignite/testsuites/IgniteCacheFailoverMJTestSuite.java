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
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.distributed.replicated.*;

/**
 * Test suite.
 */
public class IgniteCacheFailoverMJTestSuite extends TestSuite {
    /**
     * @return Ignite Cache Failover test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache Failover Test Suite");

        // Failure consistency tests.
        suite.addTestSuite(GridCacheAtomicRemoveFailureMJTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderRemoveFailureMJTest.class);
        suite.addTestSuite(GridCacheAtomicClientRemoveFailureMJTest.class);

        suite.addTestSuite(GridCacheDhtAtomicRemoveFailureMJTest.class);
        suite.addTestSuite(GridCacheDhtRemoveFailureMJTest.class);
        suite.addTestSuite(GridCacheDhtClientRemoveFailureMJTest.class);
        suite.addTestSuite(GridCacheNearRemoveFailureMJTest.class);
        suite.addTestSuite(GridCacheAtomicNearRemoveFailureMJTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderNearRemoveFailureMJTest.class);

        // From part 2
        suite.addTestSuite(GridCacheAtomicFailoverSelfMJTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderFailoverSelfMJTest.class);
        suite.addTestSuite(GridCacheAtomicReplicatedFailoverSelfMJTest.class);

        suite.addTestSuite(GridCachePartitionedFailoverSelfMJTest.class);
        suite.addTestSuite(GridCacheColocatedFailoverSelfMJTest.class);
        suite.addTestSuite(GridCacheReplicatedFailoverSelfMJTest.class);

        return suite;
    }
}
