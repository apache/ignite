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

import java.util.Set;
import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.commandline.CommandHandlerParsingTest;
import org.apache.ignite.internal.pagemem.impl.PageIdUtilsSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheUtilsSelfTest;
import org.apache.ignite.internal.util.GridArraysSelfTest;
import org.apache.ignite.internal.util.IgniteDevOnlyLogTest;
import org.apache.ignite.internal.util.IgniteExceptionRegistrySelfTest;
import org.apache.ignite.internal.util.IgniteUtilsSelfTest;
import org.apache.ignite.internal.util.nio.GridNioDelimitedBufferSelfTest;
import org.apache.ignite.internal.util.nio.GridNioSelfTest;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKeySelfTest;
import org.apache.ignite.internal.util.nio.GridNioSslSelfTest;
import org.apache.ignite.internal.util.nio.impl.GridNioFilterChainSelfTest;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMapSelfTest;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemorySelfTest;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafePartitionedMapSelfTest;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeStripedLruSefTest;
import org.apache.ignite.internal.util.tostring.CircularStringBuilderSelfTest;
import org.apache.ignite.internal.util.tostring.GridToStringBuilderSelfTest;
import org.apache.ignite.internal.util.tostring.IncludeSensitiveAtomicTest;
import org.apache.ignite.internal.util.tostring.IncludeSensitiveTransactionalTest;
import org.apache.ignite.lang.GridByteArrayListSelfTest;
import org.apache.ignite.spi.discovery.ClusterMetricsSnapshotSerializeSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.thread.GridThreadPoolExecutorServiceSelfTest;
import org.apache.ignite.thread.GridThreadTest;
import org.apache.ignite.thread.IgniteThreadPoolSizeTest;
import org.apache.ignite.util.GridConcurrentLinkedDequeMultiThreadedTest;
import org.apache.ignite.util.GridIntListSelfTest;
import org.apache.ignite.util.GridLogThrottleTest;
import org.apache.ignite.util.GridLongListSelfTest;
import org.apache.ignite.util.GridMessageCollectionTest;
import org.apache.ignite.util.GridPartitionMapSelfTest;
import org.apache.ignite.util.GridQueueSelfTest;
import org.apache.ignite.util.GridRandomSelfTest;
import org.apache.ignite.util.GridSnapshotLockSelfTest;
import org.apache.ignite.util.GridSpinReadWriteLockSelfTest;
import org.apache.ignite.util.GridStringBuilderFactorySelfTest;
import org.apache.ignite.util.GridTopologyHeapSizeSelfTest;
import org.apache.ignite.util.GridTransientTest;
import org.apache.ignite.util.mbeans.GridMBeanDisableSelfTest;
import org.apache.ignite.util.mbeans.GridMBeanExoticNamesSelfTest;
import org.apache.ignite.util.mbeans.GridMBeanSelfTest;
import org.apache.ignite.util.mbeans.WorkersControlMXBeanTest;

/**
 * Test suite for Ignite utility classes.
 */
public class IgniteUtilSelfTestSuite extends TestSuite {
    /**
     * @return Grid utility methods tests suite.
     */
    public static TestSuite suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution.
     * @return Test suite.
     */
    public static TestSuite suite(Set<Class> ignoredTests) {
        TestSuite suite = new TestSuite("Ignite Util Test Suite");

        suite.addTest(new JUnit4TestAdapter(GridThreadPoolExecutorServiceSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteThreadPoolSizeTest.class));
        GridTestUtils.addTestIfNeeded(suite, IgniteUtilsSelfTest.class, ignoredTests);
        suite.addTest(new JUnit4TestAdapter(GridSpinReadWriteLockSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridQueueSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridStringBuilderFactorySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridToStringBuilderSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CircularStringBuilderSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridByteArrayListSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMBeanSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMBeanDisableSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMBeanExoticNamesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridLongListSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridThreadTest.class));
        suite.addTest(new JUnit4TestAdapter(GridIntListSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridArraysSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheUtilsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteExceptionRegistrySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMessageCollectionTest.class));
        suite.addTest(new JUnit4TestAdapter(WorkersControlMXBeanTest.class));
        suite.addTest(new JUnit4TestAdapter(GridConcurrentLinkedDequeMultiThreadedTest.class));
        suite.addTest(new JUnit4TestAdapter(GridLogThrottleTest.class));
        suite.addTest(new JUnit4TestAdapter(GridRandomSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSnapshotLockSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTopologyHeapSizeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTransientTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDevOnlyLogTest.class));

        // Sensitive toString.
        suite.addTest(new JUnit4TestAdapter(IncludeSensitiveAtomicTest.class));
        suite.addTest(new JUnit4TestAdapter(IncludeSensitiveTransactionalTest.class));

        // Metrics.
        suite.addTest(new JUnit4TestAdapter(ClusterMetricsSnapshotSerializeSelfTest.class));

        // Unsafe.
        suite.addTest(new JUnit4TestAdapter(GridUnsafeMemorySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUnsafeStripedLruSefTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUnsafeMapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUnsafePartitionedMapSelfTest.class));

        // NIO.
        suite.addTest(new JUnit4TestAdapter(GridNioSessionMetaKeySelfTest.class));
        GridTestUtils.addTestIfNeeded(suite, GridNioSelfTest.class, ignoredTests);
        suite.addTest(new JUnit4TestAdapter(GridNioFilterChainSelfTest.class));
        GridTestUtils.addTestIfNeeded(suite, GridNioSslSelfTest.class, ignoredTests);
        suite.addTest(new JUnit4TestAdapter(GridNioDelimitedBufferSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridPartitionMapSelfTest.class));

        //dbx
        suite.addTest(new JUnit4TestAdapter(PageIdUtilsSelfTest.class));

        // control.sh
        suite.addTest(new JUnit4TestAdapter(CommandHandlerParsingTest.class));

        return suite;
    }
}
