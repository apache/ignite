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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.BinaryTypeMismatchLoggingTest;
import org.apache.ignite.internal.processors.cache.CacheBinaryKeyConcurrentQueryTest;
import org.apache.ignite.internal.processors.cache.CacheConfigurationP2PTest;
import org.apache.ignite.internal.processors.cache.CacheIndexStreamerTest;
import org.apache.ignite.internal.processors.cache.CacheOperationsWithExpirationTest;
import org.apache.ignite.internal.processors.cache.CacheQueryAfterDynamicCacheStartFailureTest;
import org.apache.ignite.internal.processors.cache.CacheQueryFilterExpiredTest;
import org.apache.ignite.internal.processors.cache.CacheRandomOperationsMultithreadedTest;
import org.apache.ignite.internal.processors.cache.ClientReconnectAfterClusterRestartTest;
import org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeSqlTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffheapIndexEntryEvictTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffheapIndexGetSelfTest;
import org.apache.ignite.internal.processors.cache.GridIndexingWithNoopSwapSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigurationPrimitiveTypesSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheGroupsSqlTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheStarvationOnRebalanceTest;
import org.apache.ignite.internal.processors.cache.IgniteClientReconnectQueriesTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlAtomicLocalSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlAtomicPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlTransactionalLocalSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlTransactionalPartitionedSelfTest;
import org.apache.ignite.internal.processors.client.IgniteDataStreamerTest;
import org.apache.ignite.internal.processors.query.h2.database.InlineIndexHelperTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Cache tests using indexing.
 */
@RunWith(AllTests.class)
public class IgniteCacheWithIndexingTestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Cache With Indexing Test Suite");

        suite.addTest(new JUnit4TestAdapter(InlineIndexHelperTest.class));

        suite.addTest(new JUnit4TestAdapter(GridIndexingWithNoopSwapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheOffHeapSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheTtlTransactionalLocalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheTtlTransactionalPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheTtlAtomicLocalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheTtlAtomicPartitionedSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheOffheapIndexGetSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheOffheapIndexEntryEvictTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheIndexStreamerTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheConfigurationP2PTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheConfigurationPrimitiveTypesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectQueriesTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheRandomOperationsMultithreadedTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheStarvationOnRebalanceTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheOperationsWithExpirationTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheBinaryKeyConcurrentQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheQueryFilterExpiredTest.class));

        suite.addTest(new JUnit4TestAdapter(ClientReconnectAfterClusterRestartTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheQueryAfterDynamicCacheStartFailureTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheGroupsSqlTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteDataStreamerTest.class));

        suite.addTest(new JUnit4TestAdapter(BinaryTypeMismatchLoggingTest.class));

        suite.addTest(new JUnit4TestAdapter(ClusterReadOnlyModeSqlTest.class));

        return suite;
    }
}
