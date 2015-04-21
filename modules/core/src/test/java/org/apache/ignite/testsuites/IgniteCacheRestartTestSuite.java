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
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.distributed.replicated.*;

/**
 * In-Memory Data Grid stability test suite on changing topology.
 */
public class IgniteCacheRestartTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache Restart Test Suite");

        suite.addTestSuite(GridCachePartitionedTxSalvageSelfTest.class);

        // TODO: GG-7419: Enable when fixed.
        // suite.addTestSuite(GridCachePartitionedNodeRestartTest.class);
        // suite.addTestSuite(GridCachePartitionedOptimisticTxNodeRestartTest.class);
        // TODO: uncomment when fix GG-1969
        // suite.addTestSuite(GridCacheReplicatedNodeRestartSelfTest.class);

        suite.addTestSuite(IgniteCacheAtomicNodeRestartTest.class);
        // suite.addTestSuite(IgniteCacheAtomicReplicatedNodeRestartSelfTest.class); // TODO IGNITE-747

        suite.addTestSuite(IgniteCacheAtomicPutAllFailoverSelfTest.class);
        // suite.addTestSuite(GridCachePutAllFailoverSelfTest.class); TODO IGNITE-157

        return suite;
    }
}
