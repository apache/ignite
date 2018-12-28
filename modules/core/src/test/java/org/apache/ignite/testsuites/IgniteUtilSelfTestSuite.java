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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

/**
 * Test suite for Ignite utility classes.
 */
@RunWith(IgniteUtilSelfTestSuite.DynamicSuite.class)
public class IgniteUtilSelfTestSuite {
    /**
     * @return Grid utility methods tests suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        suite.add(GridThreadPoolExecutorServiceSelfTest.class);
        suite.add(IgniteThreadPoolSizeTest.class);
        GridTestUtils.addTestIfNeeded(suite, IgniteUtilsSelfTest.class, ignoredTests);
        suite.add(GridSpinReadWriteLockSelfTest.class);
        suite.add(GridQueueSelfTest.class);
        suite.add(GridStringBuilderFactorySelfTest.class);
        suite.add(GridToStringBuilderSelfTest.class);
        suite.add(CircularStringBuilderSelfTest.class);
        suite.add(GridByteArrayListSelfTest.class);
        suite.add(GridMBeanSelfTest.class);
        suite.add(GridMBeanDisableSelfTest.class);
        suite.add(GridMBeanExoticNamesSelfTest.class);
        suite.add(GridLongListSelfTest.class);
        suite.add(GridThreadTest.class);
        suite.add(GridIntListSelfTest.class);
        suite.add(GridArraysSelfTest.class);
        suite.add(GridCacheUtilsSelfTest.class);
        suite.add(IgniteExceptionRegistrySelfTest.class);
        suite.add(GridMessageCollectionTest.class);
        suite.add(WorkersControlMXBeanTest.class);
        suite.add(GridConcurrentLinkedDequeMultiThreadedTest.class);
        suite.add(GridLogThrottleTest.class);
        suite.add(GridRandomSelfTest.class);
        suite.add(GridSnapshotLockSelfTest.class);
        suite.add(GridTopologyHeapSizeSelfTest.class);
        suite.add(GridTransientTest.class);
        suite.add(IgniteDevOnlyLogTest.class);

        // Sensitive toString.
        suite.add(IncludeSensitiveAtomicTest.class);
        suite.add(IncludeSensitiveTransactionalTest.class);

        // Metrics.
        suite.add(ClusterMetricsSnapshotSerializeSelfTest.class);

        // Unsafe.
        suite.add(GridUnsafeMemorySelfTest.class);
        suite.add(GridUnsafeStripedLruSefTest.class);
        suite.add(GridUnsafeMapSelfTest.class);
        suite.add(GridUnsafePartitionedMapSelfTest.class);

        // NIO.
        suite.add(GridNioSessionMetaKeySelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, GridNioSelfTest.class, ignoredTests);
        suite.add(GridNioFilterChainSelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, GridNioSslSelfTest.class, ignoredTests);
        suite.add(GridNioDelimitedBufferSelfTest.class);

        suite.add(GridPartitionMapSelfTest.class);

        //dbx
        suite.add(PageIdUtilsSelfTest.class);

        // control.sh
        suite.add(CommandHandlerParsingTest.class);

        return suite;
    }

    /** */
    public static class DynamicSuite extends Suite {
        /** */
        public DynamicSuite(Class<?> cls) throws InitializationError {
            super(cls, suite().toArray(new Class<?>[] {null}));
        }
    }
}
