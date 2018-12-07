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

import java.util.Collection;
import java.util.HashSet;
import junit.framework.TestSuite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.distributed.CacheAsyncOperationsFailoverAtomicTest;
import org.apache.ignite.internal.processors.cache.distributed.CachePutAllFailoverAtomicTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheCrossCacheTxFailoverTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicReplicatedFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxSalvageSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedFailoverSelfTest;

/**
 * Ignite Mvcc Cache Failover Suite part 2.
 */
public class IgniteMvccCacheFailoverTestSuite2 {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        Collection<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests.
        ignoredTests.add(GridCachePartitionedFailoverSelfTest.class);
        ignoredTests.add(GridCacheColocatedFailoverSelfTest.class);
        ignoredTests.add(GridCacheReplicatedFailoverSelfTest.class);
        ignoredTests.add(GridCachePartitionedTxSalvageSelfTest.class);
        ignoredTests.add(IgniteCacheCrossCacheTxFailoverTest.class);

        // Skip atomic tests.
        ignoredTests.add(GridCacheAtomicFailoverSelfTest.class);
        ignoredTests.add(GridCacheAtomicReplicatedFailoverSelfTest.class);
        ignoredTests.add(CachePutAllFailoverAtomicTest.class);
        ignoredTests.add(CacheAsyncOperationsFailoverAtomicTest.class);
        ignoredTests.add(GridCacheAtomicFailoverSelfTest.class);

        TestSuite suite = new TestSuite("Mvcc Cache Failover Test Suite2");

        suite.addTest(IgniteCacheFailoverTestSuite2.suite(ignoredTests));

        return suite;
    }
}
