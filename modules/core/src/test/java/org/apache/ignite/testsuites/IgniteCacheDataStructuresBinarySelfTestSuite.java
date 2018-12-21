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
import org.apache.ignite.internal.processors.cache.datastructures.GridCacheQueueApiSelfAbstractTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalAtomicQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.local.GridCacheLocalQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedAtomicQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedQueueApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.IgnitePartitionedQueueNoBackupsTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedQueueApiSelfTest;

/**
 * Test suite for binary cache data structures.
 */
public class IgniteCacheDataStructuresBinarySelfTestSuite extends TestSuite {
    /**
     * @return Cache test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Cache Data Structures Binary Test Suite");

        System.setProperty(GridCacheQueueApiSelfAbstractTest.BINARY_QUEUE, "true");

        suite.addTest(new TestSuite(GridCacheLocalQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalAtomicQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueApiSelfTest.class));
        suite.addTest(new TestSuite(IgnitePartitionedQueueNoBackupsTest.class));

        return suite;
    }
}
