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

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.apache.ignite.spi.discovery.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.mbeans.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.nio.impl.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.tostring.*;

/**
 * Test suite for GridGain utility classes.
 */
public class GridUtilSelfTestSuite extends TestSuite {
    /**
     * @return Grid utility methods tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Util Test Suite");

        suite.addTestSuite(GridThreadPoolExecutorServiceSelfTest.class);
        suite.addTestSuite(GridUtilsSelfTest.class);
        suite.addTestSuite(GridSpinReadWriteLockSelfTest.class);
        suite.addTestSuite(GridStringBuilderFactorySelfTest.class);
        suite.addTestSuite(GridToStringBuilderSelfTest.class);
        suite.addTestSuite(GridByteArrayListSelfTest.class);
        suite.addTestSuite(GridMBeanSelfTest.class);
        suite.addTestSuite(GridLongListSelfTest.class);
        suite.addTestSuite(GridCacheUtilsSelfTest.class);

        // Metrics.
        suite.addTestSuite(GridDiscoveryMetricsHelperSelfTest.class);

        // Unsafe.
        suite.addTestSuite(GridUnsafeMemorySelfTest.class);
        suite.addTestSuite(GridUnsafeStripedLruSefTest.class);
        suite.addTestSuite(GridUnsafeMapSelfTest.class);
        suite.addTestSuite(GridUnsafePartitionedMapSelfTest.class);

        // NIO.
        suite.addTestSuite(GridNioSessionMetaKeySelfTest.class);
        suite.addTestSuite(GridNioSelfTest.class);
        suite.addTestSuite(GridNioFilterChainSelfTest.class);
        suite.addTestSuite(GridNioSslSelfTest.class);

        return suite;
    }
}
