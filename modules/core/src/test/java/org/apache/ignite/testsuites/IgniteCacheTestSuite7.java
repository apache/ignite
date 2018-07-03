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
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateNearEnabledNoBackupsSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultinodeUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheCreatePutTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCacheWriteSynchronizationModesMultithreadedTest;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite7 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("IgniteCache Test Suite part 4");

        // Multi node update.
        suite.addTestSuite(GridCacheMultinodeUpdateSelfTest.class);
        suite.addTestSuite(GridCacheMultinodeUpdateNearEnabledSelfTest.class);
        suite.addTestSuite(GridCacheMultinodeUpdateNearEnabledNoBackupsSelfTest.class);
        suite.addTestSuite(GridCacheMultinodeUpdateAtomicSelfTest.class);
        suite.addTestSuite(GridCacheMultinodeUpdateAtomicNearEnabledSelfTest.class);

        suite.addTestSuite(IgniteTxCachePrimarySyncTest.class);
        suite.addTestSuite(IgniteTxCacheWriteSynchronizationModesMultithreadedTest.class);
        suite.addTestSuite(IgniteCacheCreatePutTest.class);

        return suite;
    }
}
