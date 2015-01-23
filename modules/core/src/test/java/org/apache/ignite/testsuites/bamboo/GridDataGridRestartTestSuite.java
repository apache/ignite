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

package org.apache.ignite.testsuites.bamboo;

import junit.framework.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;

/**
 * In-Memory Data Grid stability test suite on changing topology.
 */
public class GridDataGridRestartTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain In-Memory Data Grid Restart Test Suite");

        // Common restart tests.
        // TODO: GG-7419: Enable when fixed.
//        suite.addTestSuite(GridCachePartitionedNodeRestartTest.class);
//        suite.addTestSuite(GridCachePartitionedOptimisticTxNodeRestartTest.class);

        // TODO: uncomment when fix GG-1969
//        suite.addTestSuite(GridCacheReplicatedNodeRestartSelfTest.class);

        // The rest.
        suite.addTestSuite(GridCachePartitionedTxSalvageSelfTest.class);
        suite.addTestSuite(GridCachePutAllFailoverSelfTest.class);

        return suite;
    }
}
