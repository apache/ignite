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
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * Cache tests using indexing.
 */
public class IgniteCacheWithIndexingTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(GridAbstractTest.PERSISTENCE_IN_TESTS_IS_ALLOWED_PROPERTY, "false");
        
        TestSuite suite = new TestSuite("Ignite Cache With Indexing Test Suite");

        suite.addTestSuite(InlineIndexHelperTest.class);

        suite.addTestSuite(GridIndexingWithNoopSwapSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapSelfTest.class);

        suite.addTestSuite(CacheTtlTransactionalLocalSelfTest.class);
        suite.addTestSuite(CacheTtlTransactionalPartitionedSelfTest.class);
        suite.addTestSuite(CacheTtlAtomicLocalSelfTest.class);
        suite.addTestSuite(CacheTtlAtomicPartitionedSelfTest.class);

        suite.addTestSuite(GridCacheOffheapIndexGetSelfTest.class);
        suite.addTestSuite(GridCacheOffheapIndexEntryEvictTest.class);
        suite.addTestSuite(CacheIndexStreamerTest.class);

        suite.addTestSuite(CacheConfigurationP2PTest.class);

        suite.addTestSuite(IgniteCacheConfigurationPrimitiveTypesSelfTest.class);
        suite.addTestSuite(IgniteClientReconnectQueriesTest.class);
        suite.addTestSuite(CacheRandomOperationsMultithreadedTest.class);
        suite.addTestSuite(IgniteCacheStarvationOnRebalanceTest.class);
        suite.addTestSuite(CacheOperationsWithExpirationTest.class);
        suite.addTestSuite(CacheBinaryKeyConcurrentQueryTest.class);
        suite.addTestSuite(CacheQueryFilterExpiredTest.class);

        suite.addTestSuite(ClientReconnectAfterClusterRestartTest.class);

        suite.addTestSuite(CacheQueryAfterDynamicCacheStartFailureTest.class);

        suite.addTestSuite(IgniteCacheGroupsSqlTest.class);

        suite.addTestSuite(IgniteDataStreamerTest.class);

        suite.addTestSuite(BinaryTypeMismatchLoggingTest.class);

        suite.addTestSuite(ClusterReadOnlyModeSqlTest.class);

        return suite;
    }
}
