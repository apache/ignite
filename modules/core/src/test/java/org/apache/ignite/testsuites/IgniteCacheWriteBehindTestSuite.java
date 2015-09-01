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
import org.apache.ignite.internal.processors.cache.GridCachePartitionedWritesTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreLocalTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStorePartitionedMultiNodeSelfTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStorePartitionedTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreReplicatedTest;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStoreSelfTest;
import org.apache.ignite.internal.processors.cache.store.IgnteCacheClientWriteBehindStoreAtomicTest;
import org.apache.ignite.internal.processors.cache.store.IgnteCacheClientWriteBehindStoreTxTest;

/**
 * Test suite that contains all tests for {@link org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore}.
 */
public class IgniteCacheWriteBehindTestSuite extends TestSuite {
    /**
     * @return Ignite Bamboo in-memory data grid test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Write-Behind Store Test Suite");

        // Write-behind tests.
        suite.addTest(new TestSuite(GridCacheWriteBehindStoreSelfTest.class));
        suite.addTest(new TestSuite(GridCacheWriteBehindStoreMultithreadedSelfTest.class));
        suite.addTest(new TestSuite(GridCacheWriteBehindStoreLocalTest.class));
        suite.addTest(new TestSuite(GridCacheWriteBehindStoreReplicatedTest.class));
        suite.addTest(new TestSuite(GridCacheWriteBehindStorePartitionedTest.class));
        suite.addTest(new TestSuite(GridCacheWriteBehindStorePartitionedMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedWritesTest.class));
        suite.addTest(new TestSuite(IgnteCacheClientWriteBehindStoreAtomicTest.class));
        suite.addTest(new TestSuite(IgnteCacheClientWriteBehindStoreTxTest.class));

        return suite;
    }
}