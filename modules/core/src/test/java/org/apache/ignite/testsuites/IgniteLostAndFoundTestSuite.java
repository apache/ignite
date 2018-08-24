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
import org.apache.ignite.internal.GridFactoryVmShutdownTest;
import org.apache.ignite.internal.managers.GridManagerMxBeanIllegalArgumentHandleTest;
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheMultiNodeDataStructureTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.preloader.GridCacheReplicatedPreloadUndeploysTest;
import org.apache.ignite.internal.processors.cache.persistence.file.FileDownloaderTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesWriteThrottleSandboxTest;
import org.apache.ignite.internal.processors.compute.GridComputeJobExecutionErrorToLogManualTest;
import org.apache.ignite.internal.util.future.GridFutureQueueTest;
import org.apache.ignite.internal.util.nio.GridRoundTripTest;
import org.apache.ignite.jvmtest.BlockingQueueTest;
import org.apache.ignite.jvmtest.FileIOTest;
import org.apache.ignite.jvmtest.FileLocksTest;
import org.apache.ignite.jvmtest.LinkedHashMapTest;
import org.apache.ignite.jvmtest.MultipleFileIOTest;
import org.apache.ignite.jvmtest.NetworkFailureTest;
import org.apache.ignite.jvmtest.QueueSizeCounterMultiThreadedTest;
import org.apache.ignite.jvmtest.ReadWriteLockMultiThreadedTest;
import org.apache.ignite.jvmtest.RegExpTest;
import org.apache.ignite.jvmtest.ServerSocketMultiThreadedTest;
import org.apache.ignite.lang.GridSystemCurrentTimeMillisTest;
import org.apache.ignite.lang.GridThreadPriorityTest;
import org.apache.ignite.startup.servlet.GridServletLoaderTest;

/**
 * Tests suite for orphaned tests.
 */
public class IgniteLostAndFoundTestSuite extends TestSuite {
    /**
     * @return Tests suite for orphaned tests (not in any test sute previously).
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite List And Found Test Suite");

        suite.addTestSuite(FileIOTest.class);
        suite.addTestSuite(FileLocksTest.class);
        suite.addTestSuite(GridComputeJobExecutionErrorToLogManualTest.class);
        suite.addTestSuite(GridManagerMxBeanIllegalArgumentHandleTest.class);
        suite.addTestSuite(GridRoundTripTest.class);
        suite.addTestSuite(GridServletLoaderTest.class);

        suite.addTestSuite(LinkedHashMapTest.class);
        suite.addTestSuite(NetworkFailureTest.class);
        suite.addTestSuite(PagesWriteThrottleSandboxTest.class);
        suite.addTestSuite(QueueSizeCounterMultiThreadedTest.class);
        suite.addTestSuite(ReadWriteLockMultiThreadedTest.class);
        suite.addTestSuite(RegExpTest.class);
        suite.addTestSuite(ServerSocketMultiThreadedTest.class);


        // Non-JUnit classes with Test in name, which should be either converted to JUnit or removed in the future
        // Main classes:
        Class[] _$ = new Class[] {
            GridCacheReplicatedPreloadUndeploysTest.class,
            GridCacheMultiNodeDataStructureTest.class,
            GridFactoryVmShutdownTest.class,
            GridFutureQueueTest.class,
            GridThreadPriorityTest.class,
            GridSystemCurrentTimeMillisTest.class,
            BlockingQueueTest.class,
            MultipleFileIOTest.class
        };

        return suite;
    }
}
