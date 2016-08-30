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
import org.apache.ignite.internal.processors.cache.GridCacheCrossCacheQuerySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryIndexDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryInternalKeysSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQuerySerializationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryWrappedObjectFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheCollocatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheCrossCacheJoinRandomTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinCollocatedAndNotTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinCustomAffinityMapper;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinPartitionedAndReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinQueryConditionsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDuplicateEntityConfigurationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinNoIndexTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinPartitionedAndReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinQueryWithAffinityKeyTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLargeResultSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapEvictQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapIndexScanTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapTieredMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePartitionedQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePrimitiveFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryEvictsMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryH2IndexingLeakTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryIndexSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryLoadSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryMultiThreadedOffHeapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryOffheapEvictsMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryOffheapMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCrossCachesJoinsQueryTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicNearEnabledQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheClientQueryReplicatedNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedSnapshotEnabledQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNoRebalanceSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeFailTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest2;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQuerySelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSchemaIndexingTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSplitterSelfTest;
import org.apache.ignite.internal.processors.query.h2.sql.GridQueryParsingTest;

/**
 * Test suite for cache queries.
 */
public class IgniteCacheQuerySelfTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Cache Queries Test Suite");

        // Parsing
        suite.addTestSuite(GridQueryParsingTest.class);

        // Config.
        suite.addTestSuite(IgniteCacheDuplicateEntityConfigurationSelfTest.class);

        // Queries tests.
        suite.addTestSuite(IgniteSqlSplitterSelfTest.class);
        suite.addTestSuite(IgniteSqlSchemaIndexingTest.class);
        suite.addTestSuite(GridCacheQueryIndexDisabledSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryLoadSelfTest.class);
        suite.addTestSuite(IgniteCacheLocalQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheLocalAtomicQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheReplicatedQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheReplicatedQueryP2PDisabledSelfTest.class);
        suite.addTestSuite(IgniteCachePartitionedQuerySelfTest.class);
        suite.addTestSuite(IgniteCachePartitionedSnapshotEnabledQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheAtomicQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledQuerySelfTest.class);
        suite.addTestSuite(IgniteCachePartitionedQueryP2PDisabledSelfTest.class);
        suite.addTestSuite(IgniteCachePartitionedQueryMultiThreadedSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryIndexSelfTest.class);
        suite.addTestSuite(IgniteCacheCollocatedQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheLargeResultSelfTest.class);
        suite.addTestSuite(GridCacheQueryInternalKeysSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryMultiThreadedSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryMultiThreadedOffHeapTieredSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryEvictsMultiThreadedSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryOffheapMultiThreadedSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryOffheapEvictsMultiThreadedSelfTest.class);
        suite.addTestSuite(IgniteCacheOffheapEvictQueryTest.class);
        suite.addTestSuite(IgniteCacheSqlQueryMultiThreadedSelfTest.class);
        suite.addTestSuite(IgniteCacheOffheapTieredMultithreadedSelfTest.class);
        suite.addTestSuite(IgniteCacheOffheapIndexScanTest.class);
        suite.addTestSuite(IgniteCacheQueryNodeRestartSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryNodeRestartSelfTest2.class);
        suite.addTestSuite(IgniteCacheQueryNodeRestartDistributedJoinSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryNodeFailTest.class);
        suite.addTestSuite(IgniteCacheClientQueryReplicatedNodeRestartSelfTest.class);
        suite.addTestSuite(GridCacheCrossCacheQuerySelfTest.class);
        suite.addTestSuite(GridCacheQuerySerializationSelfTest.class);
        suite.addTestSuite(IgniteBinaryObjectFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteBinaryWrappedObjectFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheQueryH2IndexingLeakTest.class);
        suite.addTestSuite(IgniteCacheQueryNoRebalanceSelfTest.class);
        suite.addTestSuite(IgniteCachePrimitiveFieldsQuerySelfTest.class);

        suite.addTestSuite(IgniteCacheJoinQueryWithAffinityKeyTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinCollocatedAndNotTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinPartitionedAndReplicatedTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinQueryConditionsTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinTest.class);
        suite.addTestSuite(IgniteCacheJoinPartitionedAndReplicatedTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinNoIndexTest.class);
        suite.addTestSuite(IgniteCrossCachesJoinsQueryTest.class);
        suite.addTestSuite(IgniteCacheCrossCacheJoinRandomTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinCustomAffinityMapper.class);

        return suite;
    }
}
