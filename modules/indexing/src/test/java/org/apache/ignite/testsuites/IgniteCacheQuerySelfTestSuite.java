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
import org.apache.ignite.internal.processors.cache.CacheLocalQueryMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheScanPartitionQueryFallbackSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheCrossCacheQuerySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryIndexDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryIndexingDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryInternalKeysSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQuerySerializationSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReduceQueryMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryWrappedObjectFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheCollocatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDuplicateEntityConfigurationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheFieldsQueryNoDataSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLargeResultSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheNoClassQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapEvictQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapTieredMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheP2pUnmarshallingQueryErrorTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePartitionedQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryEvictsMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryIndexSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryLoadSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryMultiThreadedOffHeapTieredSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryOffheapEvictsMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryOffheapMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.SqlFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicNearEnabledFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicNearEnabledQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheClientQueryReplicatedNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedFieldsQueryP2PEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedSnapshotEnabledQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest2;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheSwapScanQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverAtomicPrimaryWriteOrderOffheapTieredTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverAtomicPrimaryWriteOrderSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverAtomicReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverTxOffheapTieredTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverTxReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverTxSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryRandomOperationsTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryAtomicOffheapTieredTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryAtomicOffheapValuesTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryAtomicP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryLocalAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryLocalSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionAtomicOneNodeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionTxOneNodeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionedOnlySelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionedP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedAtomicOneNodeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedTxOneNodeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryTxOffheapTieredTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryTxOffheapValuesTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryTxSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryClientReconnectTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryClientTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryClientTxReconnectTest;
import org.apache.ignite.internal.processors.cache.reducefields.GridCacheReduceFieldsQueryAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.reducefields.GridCacheReduceFieldsQueryLocalSelfTest;
import org.apache.ignite.internal.processors.cache.reducefields.GridCacheReduceFieldsQueryPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.reducefields.GridCacheReduceFieldsQueryReplicatedSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSchemaIndexingTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSplitterSelfTest;
import org.apache.ignite.internal.processors.query.h2.sql.BaseH2CompareQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.GridQueryParsingTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryTest;
import org.apache.ignite.spi.communication.tcp.GridOrderedMessageCancelSelfTest;

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
        suite.addTestSuite(IgniteCacheQueryNodeRestartSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryNodeRestartSelfTest2.class);
        suite.addTestSuite(IgniteCacheClientQueryReplicatedNodeRestartSelfTest.class);
        suite.addTestSuite(GridCacheReduceQueryMultithreadedSelfTest.class);
        suite.addTestSuite(GridCacheCrossCacheQuerySelfTest.class);
        suite.addTestSuite(GridCacheQuerySerializationSelfTest.class);
        suite.addTestSuite(IgniteBinaryObjectFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteBinaryWrappedObjectFieldsQuerySelfTest.class);

        // Scan queries.
        suite.addTestSuite(CacheScanPartitionQueryFallbackSelfTest.class);

        // Fields queries.
        suite.addTestSuite(SqlFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheLocalFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheReplicatedFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest.class);
        suite.addTestSuite(IgniteCachePartitionedFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheAtomicFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCachePartitionedFieldsQueryP2PEnabledSelfTest.class);
        suite.addTestSuite(IgniteCacheFieldsQueryNoDataSelfTest.class);

        // Continuous queries.
        suite.addTestSuite(GridCacheContinuousQueryLocalSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryLocalAtomicSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedAtomicSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedP2PDisabledSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionedSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionedOnlySelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionedP2PDisabledSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryTxSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryTxOffheapTieredTest.class);
        suite.addTestSuite(GridCacheContinuousQueryTxOffheapValuesTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicNearEnabledSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicP2PDisabledSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicOffheapTieredTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicOffheapValuesTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedTxOneNodeTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedAtomicOneNodeTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionTxOneNodeTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionAtomicOneNodeTest.class);
        suite.addTestSuite(IgniteCacheContinuousQueryClientTest.class);
        suite.addTestSuite(IgniteCacheContinuousQueryClientReconnectTest.class);
        suite.addTestSuite(IgniteCacheContinuousQueryClientTxReconnectTest.class);
        suite.addTestSuite(CacheContinuousQueryFailoverAtomicPrimaryWriteOrderSelfTest.class);
        suite.addTestSuite(CacheContinuousQueryFailoverAtomicReplicatedSelfTest.class);
        suite.addTestSuite(CacheContinuousQueryFailoverTxSelfTest.class);
        suite.addTestSuite(CacheContinuousQueryFailoverTxReplicatedSelfTest.class);
        suite.addTestSuite(CacheContinuousQueryFailoverAtomicPrimaryWriteOrderOffheapTieredTest.class);
        suite.addTestSuite(CacheContinuousQueryFailoverTxOffheapTieredTest.class);
        suite.addTestSuite(CacheContinuousQueryRandomOperationsTest.class);

        // Reduce fields queries.
        suite.addTestSuite(GridCacheReduceFieldsQueryLocalSelfTest.class);
        suite.addTestSuite(GridCacheReduceFieldsQueryPartitionedSelfTest.class);
        suite.addTestSuite(GridCacheReduceFieldsQueryAtomicSelfTest.class);
        suite.addTestSuite(GridCacheReduceFieldsQueryReplicatedSelfTest.class);

        suite.addTestSuite(GridCacheQueryIndexingDisabledSelfTest.class);

        suite.addTestSuite(GridCacheSwapScanQuerySelfTest.class);

        suite.addTestSuite(GridOrderedMessageCancelSelfTest.class);

        // Ignite cache and H2 comparison.
        suite.addTestSuite(BaseH2CompareQueryTest.class);
        suite.addTestSuite(H2CompareBigQueryTest.class);

        // Cache query metrics.
        suite.addTestSuite(CacheLocalQueryMetricsSelfTest.class);
        suite.addTestSuite(CachePartitionedQueryMetricsDistributedSelfTest.class);
        suite.addTestSuite(CachePartitionedQueryMetricsLocalSelfTest.class);
        suite.addTestSuite(CacheReplicatedQueryMetricsDistributedSelfTest.class);
        suite.addTestSuite(CacheReplicatedQueryMetricsLocalSelfTest.class);

        //Unmarshallig query test.
        suite.addTestSuite(IgniteCacheP2pUnmarshallingQueryErrorTest.class);
        suite.addTestSuite(IgniteCacheNoClassQuerySelfTest.class);

        return suite;
    }
}
