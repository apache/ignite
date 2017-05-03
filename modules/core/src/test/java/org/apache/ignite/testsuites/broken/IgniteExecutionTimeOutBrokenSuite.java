/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites.broken;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.distributed.CachePutAllFailoverTxTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOptimisticTxNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteAtomicLongChangingTopologySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNodeRestartTest;

/**
 *
 */
public class IgniteExecutionTimeOutBrokenSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Execution Time Out Broken Test Suite");

        suite.addTestSuite(GridCachePartitionedNodeRestartTest.class);
        suite.addTestSuite(CachePutAllFailoverTxTest.class);
        suite.addTestSuite(IgniteAtomicLongChangingTopologySelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledOptimisticTxNodeRestartTest.class);

        return suite;
    }
}
