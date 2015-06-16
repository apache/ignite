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
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.nio.*;
import org.apache.ignite.internal.util.nio.impl.*;
import org.apache.ignite.internal.util.offheap.unsafe.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.thread.*;
import org.apache.ignite.util.*;
import org.apache.ignite.util.mbeans.*;

import java.util.*;

/**
 * Test suite for Ignite utility classes.
 */
public class IgniteUtilSelfTestSuite extends TestSuite {
    /**
     * @return Grid utility methods tests suite.
     * @throws Exception If failed.
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
        TestSuite suite = new TestSuite("Ignite Util Test Suite");

        suite.addTestSuite(GridThreadPoolExecutorServiceSelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, IgniteUtilsSelfTest.class, ignoredTests);
        suite.addTestSuite(GridSpinReadWriteLockSelfTest.class);
        suite.addTestSuite(GridQueueSelfTest.class);
        suite.addTestSuite(GridStringBuilderFactorySelfTest.class);
        suite.addTestSuite(GridToStringBuilderSelfTest.class);
        suite.addTestSuite(GridByteArrayListSelfTest.class);
        suite.addTestSuite(GridMBeanSelfTest.class);
        suite.addTestSuite(GridLongListSelfTest.class);
        suite.addTestSuite(GridCacheUtilsSelfTest.class);
        suite.addTestSuite(IgniteExceptionRegistrySelfTest.class);

        // Metrics.
        suite.addTestSuite(ClusterMetricsSnapshotSerializeSelfTest.class);

        // Unsafe.
        suite.addTestSuite(GridUnsafeMemorySelfTest.class);
        suite.addTestSuite(GridUnsafeStripedLruSefTest.class);
        suite.addTestSuite(GridUnsafeMapSelfTest.class);
        suite.addTestSuite(GridUnsafePartitionedMapSelfTest.class);

        // NIO.
        suite.addTestSuite(GridNioSessionMetaKeySelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, GridNioSelfTest.class, ignoredTests);
        suite.addTestSuite(GridNioFilterChainSelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, GridNioSslSelfTest.class, ignoredTests);
        suite.addTestSuite(GridNioDelimitedBufferSelfTest.class);

        return suite;
    }
}
