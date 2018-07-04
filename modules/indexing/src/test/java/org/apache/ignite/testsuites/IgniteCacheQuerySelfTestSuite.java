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
import org.apache.ignite.internal.processors.cache.CacheIteratorScanQueryTest;
import org.apache.ignite.internal.processors.cache.CacheLocalQueryDetailMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.CacheLocalQueryMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.CacheOffheapBatchIndexingSingleTypeTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryDetailMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryDetailMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheQueryEvictDataLostTest;
import org.apache.ignite.internal.processors.cache.CacheQueryNewClientSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryDetailMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryDetailMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheSqlQueryValueCopySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheCrossCacheQuerySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheFullTextQuerySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheLazyQueryPartitionsReleaseTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryIndexDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryIndexingDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryInternalKeysSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQuerySerializationSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQuerySqlFieldInlineSizeSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectLocalQueryArgumentsTest;
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
import org.apache.ignite.internal.processors.cache.IgniteCacheFullTextQueryNodeJoiningSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInsertSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinPartitionedAndReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinQueryWithAffinityKeyTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLargeResultSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheMergeSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheMultipleIndexedTypesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheNoClassQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapEvictQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapIndexScanTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheP2pUnmarshallingQueryErrorTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePrimitiveFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryH2IndexingLeakTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryIndexSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryLoadSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlQueryErrorSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheUpdateSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCheckClusterStateBeforeExecuteQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteCrossCachesJoinsQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicSqlRestoreTest;
import org.apache.ignite.internal.processors.cache.IncorrectQueryEntityTest;
import org.apache.ignite.internal.processors.cache.QueryEntityCaseMismatchTest;
import org.apache.ignite.internal.processors.cache.SqlFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.authentication.SqlUserCommandSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicNearEnabledFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicNearEnabledQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedPartitionQueryConfigurationSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedPartitionQueryNodeRestartsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedPartitionQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedQueryCancelSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedFieldsQueryP2PEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQueryEvtsDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedSnapshotEnabledQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryAbstractDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNoRebalanceSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryJoinNoPrimaryPartitionsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryROSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQueryEvtsDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.index.DuplicateKeyValueClassesSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexClientBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerCoordinatorBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerNodeFIlterBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerNodeFilterCoordinatorBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2ConnectionLeaksSelfTest;
import org.apache.ignite.internal.processors.cache.index.IgniteDecimalSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicColumnsClientBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicColumnsServerBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicColumnsServerCoordinatorBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexAtomicPartitionedNearSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexAtomicPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexAtomicReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexTransactionalPartitionedNearSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexTransactionalPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexTransactionalReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientAtomicPartitionedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientAtomicReplicatedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientTransactionalPartitionedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientTransactionalReplicatedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerAtomicPartitionedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerAtomicReplicatedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerTransactionalPartitionedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerTransactionalReplicatedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicTableSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2RowCachePageEvictionTest;
import org.apache.ignite.internal.processors.cache.index.H2RowCacheSelfTest;
import org.apache.ignite.internal.processors.cache.index.LongIndexNameTest;
import org.apache.ignite.internal.processors.cache.index.OptimizedMarshallerIndexNameTest;
import org.apache.ignite.internal.processors.cache.index.SchemaExchangeSelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQueryCancelOrTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryTransformerSelfTest;
import org.apache.ignite.internal.processors.cache.query.IgniteCacheQueryCacheDestroySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQueryTxSelfTest;
import org.apache.ignite.internal.processors.client.ClientConnectorConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteStableBaselineBinObjFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.query.IgniteCachelessQueriesSelfTest;
import org.apache.ignite.internal.processors.query.IgniteQueryDedicatedPoolTest;
import org.apache.ignite.internal.processors.query.IgniteSqlDefaultValueTest;
import org.apache.ignite.internal.processors.query.IgniteSqlDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlEntryCacheModeAgnosticTest;
import org.apache.ignite.internal.processors.query.IgniteSqlKeyValueFieldsTest;
import org.apache.ignite.internal.processors.query.IgniteSqlNotNullConstraintTest;
import org.apache.ignite.internal.processors.query.IgniteSqlParameterizedQueryTest;
import org.apache.ignite.internal.processors.query.IgniteSqlRoutingTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSchemaIndexingTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSegmentedIndexMultiNodeSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSegmentedIndexSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSkipReducerOnUpdateDmlFlagSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSkipReducerOnUpdateDmlSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSplitterSelfTest;
import org.apache.ignite.internal.processors.query.LazyQuerySelfTest;
import org.apache.ignite.internal.processors.query.MultipleStatementsSqlQuerySelfTest;
import org.apache.ignite.internal.processors.query.SqlPushDownFunctionTest;
import org.apache.ignite.internal.processors.query.SqlSchemaSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridH2IndexingInMemSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridH2IndexingOffheapSelfTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlBigIntegerKeyTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlQueryMinMaxTest;
import org.apache.ignite.internal.processors.query.h2.sql.BaseH2CompareQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.GridQueryParsingTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryDistributedJoinsTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryTest;
import org.apache.ignite.internal.processors.sql.SqlConnectorConfigurationValidationSelfTest;
import org.apache.ignite.internal.sql.SqlParserBulkLoadSelfTest;
import org.apache.ignite.internal.sql.SqlParserCreateIndexSelfTest;
import org.apache.ignite.internal.sql.SqlParserDropIndexSelfTest;
import org.apache.ignite.internal.sql.SqlParserSetStreamingSelfTest;
import org.apache.ignite.internal.sql.SqlParserUserSelfTest;
import org.apache.ignite.spi.communication.tcp.GridOrderedMessageCancelSelfTest;
import org.apache.ignite.sqltests.PartitionedSqlTest;
import org.apache.ignite.sqltests.ReplicatedSqlTest;
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

        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);

        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);

        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);

        return suite;
    }
}
