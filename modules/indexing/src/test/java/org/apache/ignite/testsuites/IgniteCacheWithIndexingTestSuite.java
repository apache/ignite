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
import org.apache.ignite.internal.processors.cache.CacheConfigurationP2PTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapAndSwapSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffheapIndexEntryEvictTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffheapIndexGetSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheSwapSelfTest;
import org.apache.ignite.internal.processors.cache.GridIndexingWithNoopSwapSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigurationPrimitiveTypesSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteClientReconnectQueriesTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlOffheapAtomicLocalSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlOffheapAtomicPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlOffheapTransactionalLocalSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlOffheapTransactionalPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlOnheapAtomicLocalSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlOnheapAtomicPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlOnheapTransactionalLocalSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlOnheapTransactionalPartitionedSelfTest;

/**
 * Cache tests using indexing.
 */
public class IgniteCacheWithIndexingTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Cache With Indexing Test Suite");

        suite.addTestSuite(GridCacheOffHeapAndSwapSelfTest.class);
        suite.addTestSuite(GridIndexingWithNoopSwapSelfTest.class);
        suite.addTestSuite(GridCacheSwapSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapSelfTest.class);

        suite.addTestSuite(CacheTtlOffheapAtomicLocalSelfTest.class);
        suite.addTestSuite(CacheTtlOffheapAtomicPartitionedSelfTest.class);
        suite.addTestSuite(CacheTtlOffheapTransactionalLocalSelfTest.class);
        suite.addTestSuite(CacheTtlOffheapTransactionalPartitionedSelfTest.class);
        suite.addTestSuite(CacheTtlOnheapTransactionalLocalSelfTest.class);
        suite.addTestSuite(CacheTtlOnheapTransactionalPartitionedSelfTest.class);
        suite.addTestSuite(CacheTtlOnheapAtomicLocalSelfTest.class);
        suite.addTestSuite(CacheTtlOnheapAtomicPartitionedSelfTest.class);

        suite.addTestSuite(GridCacheOffheapIndexGetSelfTest.class);
        suite.addTestSuite(GridCacheOffheapIndexEntryEvictTest.class);

        suite.addTestSuite(CacheConfigurationP2PTest.class);

        suite.addTestSuite(IgniteCacheConfigurationPrimitiveTypesSelfTest.class);
        suite.addTestSuite(IgniteClientReconnectQueriesTest.class);

        return suite;
    }
}