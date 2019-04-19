/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.IgniteCacheCreateRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheNearRestartRollbackSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOptimisticTxNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOptimisticTxNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedNodeRestartSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Cache stability test suite on changing topology.
 */
@RunWith(AllTests.class)
public class IgniteCacheRestartTestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Cache Restart Test Suite");

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNodeRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedOptimisticTxNodeRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedNodeRestartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledOptimisticTxNodeRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheNearRestartRollbackSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheCreateRestartSelfTest.class));

        return suite;
    }
}
