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
import org.apache.ignite.internal.processors.cache.CacheLocalQueryDetailMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.CacheLocalQueryMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.CacheOffheapBatchIndexingSingleTypeTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryDetailMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryDetailMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheQueryNewClientSelfTest;
import org.apache.ignite.internal.processors.cache.CacheQueryOffheapEvictDataLostTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryDetailMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryDetailMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheSqlQueryValueCopySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheCrossCacheQuerySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryIndexDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryIndexingDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryInternalKeysSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQuerySerializationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectLocalQueryArgumentsTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectQueryArgumentsOffheapLocalTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectQueryArgumentsOffheapTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectQueryArgumentsTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryWrappedObjectFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheCollocatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDeleteSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinCollocatedAndNotTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinCustomAffinityMapper;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinNoIndexTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinPartitionedAndReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinQueryConditionsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDuplicateEntityConfigurationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheFieldsQueryNoDataSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInsertSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinPartitionedAndReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinQueryWithAffinityKeyTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLargeResultSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheMergeSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheNoClassQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapEvictQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapIndexScanTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheP2pUnmarshallingQueryErrorTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePrimitiveFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryH2IndexingLeakTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryIndexSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryLoadSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheUpdateSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCrossCachesJoinsQueryTest;
import org.apache.ignite.internal.processors.cache.QueryEntityCaseMismatchTest;
import org.apache.ignite.internal.processors.cache.SqlFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicNearEnabledFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicNearEnabledQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedQueryCancelSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedFieldsQueryP2PEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedSnapshotEnabledQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryAbstractDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNoRebalanceSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQueryCancelOrTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryTransformerSelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheSwapScanQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.IgniteCacheQueryCacheDestroySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQueryTxSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlEntryCacheModeAgnosticTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSchemaIndexingTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSplitterSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridH2IndexRebuildTest;
import org.apache.ignite.internal.processors.query.h2.GridH2IndexingInMemSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridH2IndexingOffheapSelfTest;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2TableSelfTest;
import org.apache.ignite.internal.processors.query.h2.sql.BaseH2CompareQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.GridQueryParsingTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryTest;
import org.apache.ignite.spi.communication.tcp.GridOrderedMessageCancelSelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 * Test suite for cache queries.
 */
public class IgniteCacheQuerySelfTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        IgniteTestSuite suite = new IgniteTestSuite("Ignite Cache Queries Test Suite");

        // H2 tests.
        suite.addTest(new TestSuite(GridH2TableSelfTest.class));
        suite.addTest(new TestSuite(GridH2IndexingInMemSelfTest.class));
        suite.addTest(new TestSuite(GridH2IndexingOffheapSelfTest.class));
        suite.addTest(new TestSuite(GridH2IndexRebuildTest.class));

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

        suite.addTestSuite(IgniteCacheQueryIndexSelfTest.class);
        suite.addTestSuite(IgniteCacheCollocatedQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheLargeResultSelfTest.class);
        suite.addTestSuite(GridCacheQueryInternalKeysSelfTest.class);

        suite.addTestSuite(IgniteCacheOffheapEvictQueryTest.class);
        suite.addTestSuite(IgniteCacheOffheapIndexScanTest.class);

        suite.addTestSuite(IgniteCacheQueryAbstractDistributedJoinSelfTest.class);

        suite.addTestSuite(GridCacheCrossCacheQuerySelfTest.class);
        suite.addTestSuite(GridCacheQuerySerializationSelfTest.class);
        suite.addTestSuite(IgniteBinaryObjectFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteBinaryWrappedObjectFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheQueryH2IndexingLeakTest.class);
        suite.addTestSuite(IgniteCacheQueryNoRebalanceSelfTest.class);
        suite.addTestSuite(GridCacheQueryTransformerSelfTest.class);
        suite.addTestSuite(IgniteCachePrimitiveFieldsQuerySelfTest.class);

        suite.addTestSuite(IgniteCacheJoinQueryWithAffinityKeyTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinCollocatedAndNotTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinPartitionedAndReplicatedTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinQueryConditionsTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinTest.class);
        suite.addTestSuite(IgniteCacheJoinPartitionedAndReplicatedTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinNoIndexTest.class);
        suite.addTestSuite(IgniteCrossCachesJoinsQueryTest.class);

        suite.addTestSuite(IgniteCacheDistributedJoinCustomAffinityMapper.class);

        suite.addTestSuite(IgniteCacheMergeSqlQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheInsertSqlQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheUpdateSqlQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheDeleteSqlQuerySelfTest.class);

        suite.addTestSuite(IgniteBinaryObjectQueryArgumentsTest.class);
        suite.addTestSuite(IgniteBinaryObjectQueryArgumentsOffheapTest.class);
        suite.addTestSuite(IgniteBinaryObjectQueryArgumentsOffheapLocalTest.class);
        suite.addTestSuite(IgniteBinaryObjectLocalQueryArgumentsTest.class);

        suite.addTestSuite(IndexingSpiQuerySelfTest.class);
        suite.addTestSuite(IndexingSpiQueryTxSelfTest.class);

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

        suite.addTestSuite(GridCacheQueryIndexingDisabledSelfTest.class);

        suite.addTestSuite(GridCacheSwapScanQuerySelfTest.class);

        suite.addTestSuite(GridOrderedMessageCancelSelfTest.class);

        suite.addTestSuite(CacheQueryOffheapEvictDataLostTest.class);

        // Ignite cache and H2 comparison.
        suite.addTestSuite(BaseH2CompareQueryTest.class);
        suite.addTestSuite(H2CompareBigQueryTest.class);

        // Cache query metrics.
        suite.addTestSuite(CacheLocalQueryMetricsSelfTest.class);
        suite.addTestSuite(CachePartitionedQueryMetricsDistributedSelfTest.class);
        suite.addTestSuite(CachePartitionedQueryMetricsLocalSelfTest.class);
        suite.addTestSuite(CacheReplicatedQueryMetricsDistributedSelfTest.class);
        suite.addTestSuite(CacheReplicatedQueryMetricsLocalSelfTest.class);

        // Cache query metrics.
        suite.addTestSuite(CacheLocalQueryDetailMetricsSelfTest.class);
        suite.addTestSuite(CachePartitionedQueryDetailMetricsDistributedSelfTest.class);
        suite.addTestSuite(CachePartitionedQueryDetailMetricsLocalSelfTest.class);
        suite.addTestSuite(CacheReplicatedQueryDetailMetricsDistributedSelfTest.class);
        suite.addTestSuite(CacheReplicatedQueryDetailMetricsLocalSelfTest.class);

        // Unmarshalling query test.
        suite.addTestSuite(IgniteCacheP2pUnmarshallingQueryErrorTest.class);
        suite.addTestSuite(IgniteCacheNoClassQuerySelfTest.class);

        // Cancellation.

        suite.addTestSuite(IgniteCacheDistributedQueryCancelSelfTest.class);
        suite.addTestSuite(IgniteCacheLocalQueryCancelOrTimeoutSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest.class);

        // Other.
        suite.addTestSuite(CacheQueryNewClientSelfTest.class);
        suite.addTestSuite(CacheOffheapBatchIndexingSingleTypeTest.class);
        suite.addTestSuite(CacheSqlQueryValueCopySelfTest.class);
        suite.addTestSuite(IgniteCacheQueryCacheDestroySelfTest.class);
        suite.addTestSuite(IgniteSqlEntryCacheModeAgnosticTest.class);
        suite.addTestSuite(QueryEntityCaseMismatchTest.class);

        return suite;
    }
}
