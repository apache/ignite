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
import org.apache.ignite.internal.processors.cache.IgniteCacheCreateRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheNearRestartRollbackSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOptimisticTxNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOptimisticTxNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedNodeRestartSelfTest;

/**
 * Cache stability test suite on changing topology.
 */
public class IgniteCacheRestartTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache Restart Test Suite");

        suite.addTestSuite(GridCachePartitionedNodeRestartTest.class);
        suite.addTestSuite(GridCachePartitionedOptimisticTxNodeRestartTest.class);
        suite.addTestSuite(GridCacheReplicatedNodeRestartSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledOptimisticTxNodeRestartTest.class);
        suite.addTestSuite(IgniteCacheNearRestartRollbackSelfTest.class);

        suite.addTestSuite(IgniteCacheCreateRestartSelfTest.class);

        return suite;
    }
}