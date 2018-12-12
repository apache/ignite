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
import org.apache.ignite.internal.processors.cache.AffinityKeyNameAndValueFieldNameConflictTest;
import org.apache.ignite.internal.processors.cache.BigEntryQueryTest;
import org.apache.ignite.internal.processors.cache.BinaryMetadataConcurrentUpdateWithIndexesTest;
import org.apache.ignite.internal.processors.cache.BinarySerializationQuerySelfTest;
import org.apache.ignite.internal.processors.cache.BinarySerializationQueryWithReflectiveSerializerSelfTest;
import org.apache.ignite.internal.processors.cache.CacheIteratorScanQueryTest;
import org.apache.ignite.internal.processors.cache.CacheLocalQueryDetailMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.CacheLocalQueryMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.CacheOffheapBatchIndexingMultiTypeTest;
import org.apache.ignite.internal.processors.cache.CacheOffheapBatchIndexingSingleTypeTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryDetailMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryDetailMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CachePartitionedQueryMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheQueryBuildValueTest;
import org.apache.ignite.internal.processors.cache.CacheQueryEvictDataLostTest;
import org.apache.ignite.internal.processors.cache.CacheQueryNewClientSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryDetailMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryDetailMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryMetricsDistributedSelfTest;
import org.apache.ignite.internal.processors.cache.CacheReplicatedQueryMetricsLocalSelfTest;
import org.apache.ignite.internal.processors.cache.CacheSqlQueryValueCopySelfTest;
import org.apache.ignite.internal.processors.cache.DdlTransactionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheCrossCacheQuerySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheDynamicLoadOnClientPersistentTest;
import org.apache.ignite.internal.processors.cache.GridCacheDynamicLoadOnClientTest;
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
import org.apache.ignite.internal.processors.cache.IgniteCacheBinaryObjectsScanSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheBinaryObjectsScanWithEventsSelfTest;
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
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinPartitionedAndReplicatedCollocationTest;
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
import org.apache.ignite.internal.processors.cache.IgniteCacheUnionDuplicatesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheUpdateSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCheckClusterStateBeforeExecuteQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteClientReconnectCacheQueriesFailoverTest;
import org.apache.ignite.internal.processors.cache.IgniteCrossCachesJoinsQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicSqlRestoreTest;
import org.apache.ignite.internal.processors.cache.IgniteErrorOnRebalanceTest;
import org.apache.ignite.internal.processors.cache.IncorrectQueryEntityTest;
import org.apache.ignite.internal.processors.cache.IndexingCachePartitionLossPolicySelfTest;
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
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNoRebalanceSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryJoinNoPrimaryPartitionsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryROSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQueryEvtsDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.encryption.EncryptedSqlTableTest;
import org.apache.ignite.internal.processors.cache.index.BasicIndexTest;
import org.apache.ignite.internal.processors.cache.index.ComplexPrimaryKeyUnwrapSelfTest;
import org.apache.ignite.internal.processors.cache.index.DuplicateKeyValueClassesSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexClientBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerCoordinatorBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerNodeFIlterBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerNodeFilterCoordinatorBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2ConnectionLeaksSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicColumnsClientBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicColumnsServerBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicColumnsServerCoordinatorBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexAtomicPartitionedNearSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexAtomicPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexAtomicReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexTransactionalPartitionedNearSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexTransactionalPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexTransactionalReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientAtomicPartitionedNoBackupsTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientAtomicPartitionedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientAtomicReplicatedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientTransactionalPartitionedNoBackupsTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientTransactionalPartitionedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientTransactionalReplicatedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerAtomicPartitionedNoBackupsTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerAtomicPartitionedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerAtomicReplicatedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerTransactionalPartitionedNoBackupsTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerTransactionalPartitionedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerTransactionalReplicatedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicTableSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2RowCachePageEvictionTest;
import org.apache.ignite.internal.processors.cache.index.H2RowCacheSelfTest;
import org.apache.ignite.internal.processors.cache.index.IgniteDecimalSelfTest;
import org.apache.ignite.internal.processors.cache.index.LongIndexNameTest;
import org.apache.ignite.internal.processors.cache.index.OptimizedMarshallerIndexNameTest;
import org.apache.ignite.internal.processors.cache.index.QueryEntityValidationSelfTest;
import org.apache.ignite.internal.processors.cache.index.SchemaExchangeSelfTest;
import org.apache.ignite.internal.processors.cache.index.SqlTransactionCommandsWithMvccDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQueryCancelOrTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.CacheScanQueryFailoverTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryTransformerSelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCircularQueueTest;
import org.apache.ignite.internal.processors.cache.query.IgniteCacheQueryCacheDestroySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQueryTxSelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQueryWithH2IndexingSelfTest;
import org.apache.ignite.internal.processors.cache.transaction.DmlInsideTransactionTest;
import org.apache.ignite.internal.processors.client.ClientConnectorConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteStableBaselineBinObjFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.query.IgniteCachelessQueriesSelfTest;
import org.apache.ignite.internal.processors.query.IgniteQueryDedicatedPoolTest;
import org.apache.ignite.internal.processors.query.IgniteSqlDefaultValueTest;
import org.apache.ignite.internal.processors.query.IgniteSqlDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlEntryCacheModeAgnosticTest;
import org.apache.ignite.internal.processors.query.IgniteSqlGroupConcatCollocatedTest;
import org.apache.ignite.internal.processors.query.IgniteSqlGroupConcatNotCollocatedTest;
import org.apache.ignite.internal.processors.query.IgniteSqlKeyValueFieldsTest;
import org.apache.ignite.internal.processors.query.IgniteSqlNotNullConstraintTest;
import org.apache.ignite.internal.processors.query.IgniteSqlParameterizedQueryTest;
import org.apache.ignite.internal.processors.query.IgniteSqlQueryParallelismTest;
import org.apache.ignite.internal.processors.query.IgniteSqlRoutingTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSchemaIndexingTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSegmentedIndexMultiNodeSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSegmentedIndexSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSkipReducerOnUpdateDmlFlagSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSkipReducerOnUpdateDmlSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSplitterSelfTest;
import org.apache.ignite.internal.processors.query.LazyQuerySelfTest;
import org.apache.ignite.internal.processors.query.MultipleStatementsSqlQuerySelfTest;
import org.apache.ignite.internal.processors.query.RunningQueriesTest;
import org.apache.ignite.internal.processors.query.SqlIllegalSchemaSelfTest;
import org.apache.ignite.internal.processors.query.SqlNestedQuerySelfTest;
import org.apache.ignite.internal.processors.query.SqlPushDownFunctionTest;
import org.apache.ignite.internal.processors.query.SqlSchemaSelfTest;
import org.apache.ignite.internal.processors.query.SqlSystemViewsSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridH2IndexingInMemSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridH2IndexingOffheapSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridIndexRebuildSelfTest;
import org.apache.ignite.internal.processors.query.h2.H2ResultSetIteratorNullifyOnEndSelfTest;
import org.apache.ignite.internal.processors.query.h2.H2StatementCacheSelfTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlBigIntegerKeyTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlQueryMinMaxTest;
import org.apache.ignite.internal.processors.query.h2.PreparedStatementExSelfTest;
import org.apache.ignite.internal.processors.query.h2.ThreadLocalObjectPoolSelfTest;
import org.apache.ignite.internal.processors.query.h2.sql.BaseH2CompareQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.ExplainSelfTest;
import org.apache.ignite.internal.processors.query.h2.sql.GridQueryParsingTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryDistributedJoinsTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryTest;
import org.apache.ignite.internal.processors.query.h2.twostep.AndOperationExtractPartitionSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.InOperationExtractPartitionSelfTest;
import org.apache.ignite.internal.processors.sql.IgniteCachePartitionedAtomicColumnConstraintsTest;
import org.apache.ignite.internal.processors.sql.IgniteCachePartitionedTransactionalColumnConstraintsTest;
import org.apache.ignite.internal.processors.sql.IgniteCachePartitionedTransactionalSnapshotColumnConstraintTest;
import org.apache.ignite.internal.processors.sql.IgniteCacheReplicatedAtomicColumnConstraintsTest;
import org.apache.ignite.internal.processors.sql.IgniteCacheReplicatedTransactionalColumnConstraintsTest;
import org.apache.ignite.internal.processors.sql.IgniteCacheReplicatedTransactionalSnapshotColumnConstraintTest;
import org.apache.ignite.internal.processors.sql.IgniteSQLColumnConstraintsTest;
import org.apache.ignite.internal.processors.sql.IgniteTransactionSQLColumnConstraintTest;
import org.apache.ignite.internal.processors.sql.SqlConnectorConfigurationValidationSelfTest;
import org.apache.ignite.internal.sql.SqlParserBulkLoadSelfTest;
import org.apache.ignite.internal.sql.SqlParserCreateIndexSelfTest;
import org.apache.ignite.internal.sql.SqlParserDropIndexSelfTest;
import org.apache.ignite.internal.sql.SqlParserSetStreamingSelfTest;
import org.apache.ignite.internal.sql.SqlParserTransactionalKeywordsSelfTest;
import org.apache.ignite.internal.sql.SqlParserUserSelfTest;
import org.apache.ignite.spi.communication.tcp.GridOrderedMessageCancelSelfTest;
import org.apache.ignite.sqltests.PartitionedSqlTest;
import org.apache.ignite.sqltests.ReplicatedSqlTest;
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 * Test suite for cache queries.
 */
public class IgniteBinaryCacheQueryTestSuite extends TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        IgniteTestSuite suite = new IgniteTestSuite("Ignite Cache Queries Test Suite");

        suite.addTest(new JUnit4TestAdapter(AffinityKeyNameAndValueFieldNameConflictTest.class));
        suite.addTest(new JUnit4TestAdapter(DmlInsideTransactionTest.class));
        suite.addTest(new JUnit4TestAdapter(ComplexPrimaryKeyUnwrapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SqlNestedQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ExplainSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(RunningQueriesTest.class));

        suite.addTest(new JUnit4TestAdapter(ComplexPrimaryKeyUnwrapSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(PartitionedSqlTest.class));
        suite.addTest(new JUnit4TestAdapter(ReplicatedSqlTest.class));

        suite.addTest(new JUnit4TestAdapter(SqlParserCreateIndexSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SqlParserDropIndexSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SqlParserTransactionalKeywordsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SqlParserBulkLoadSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SqlParserSetStreamingSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(SqlConnectorConfigurationValidationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientConnectorConfigurationValidationSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(SqlSchemaSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SqlIllegalSchemaSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(MultipleStatementsSqlQuerySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(BasicIndexTest.class));

        // Misc tests.
        suite.addTest(new JUnit4TestAdapter(QueryEntityValidationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DuplicateKeyValueClassesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheLazyQueryPartitionsReleaseTest.class));

        // Dynamic index create/drop tests.
        suite.addTest(new JUnit4TestAdapter(SchemaExchangeSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(DynamicIndexServerCoordinatorBasicSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DynamicIndexServerBasicSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DynamicIndexServerNodeFilterCoordinatorBasicSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DynamicIndexServerNodeFIlterBasicSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DynamicIndexClientBasicSelfTest.class));

        // H2 tests.

        suite.addTest(new JUnit4TestAdapter(GridH2IndexingInMemSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridH2IndexingOffheapSelfTest.class));

        // Parsing
        suite.addTest(new JUnit4TestAdapter(GridQueryParsingTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheSqlQueryErrorSelfTest.class));

        // Config.
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDuplicateEntityConfigurationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IncorrectQueryEntityTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDynamicSqlRestoreTest.class));

        // Queries tests.
        suite.addTest(new JUnit4TestAdapter(LazyQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlSplitterSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SqlPushDownFunctionTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlSegmentedIndexSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachelessQueriesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlSegmentedIndexMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlSchemaIndexingTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheQueryIndexDisabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryLoadSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheLocalQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheLocalAtomicQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheReplicatedQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheReplicatedQueryP2PDisabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheReplicatedQueryEvtsDisabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedSnapshotEnabledQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNearEnabledQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedQueryP2PDisabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedQueryEvtsDisabledSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheUnionDuplicatesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheJoinPartitionedAndReplicatedCollocationTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectCacheQueriesFailoverTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteErrorOnRebalanceTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheQueryBuildValueTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheOffheapBatchIndexingMultiTypeTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryIndexSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheCollocatedQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheLargeResultSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheQueryInternalKeysSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2ResultSetIteratorNullifyOnEndSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlBigIntegerKeyTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheOffheapEvictQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheOffheapIndexScanTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheCrossCacheQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheQuerySerializationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteBinaryObjectFieldsQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteStableBaselineBinObjFieldsQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteBinaryWrappedObjectFieldsQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryH2IndexingLeakTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryNoRebalanceSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheQueryTransformerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheScanQueryFailoverTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePrimitiveFieldsQuerySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheJoinQueryWithAffinityKeyTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheJoinPartitionedAndReplicatedTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCrossCachesJoinsQueryTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheMultipleIndexedTypesTest.class));

        // DML.
        suite.addTest(new JUnit4TestAdapter(IgniteCacheMergeSqlQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheInsertSqlQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheUpdateSqlQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDeleteSqlQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlSkipReducerOnUpdateDmlSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlSkipReducerOnUpdateDmlFlagSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteBinaryObjectQueryArgumentsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteBinaryObjectLocalQueryArgumentsTest.class));

        suite.addTest(new JUnit4TestAdapter(IndexingSpiQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IndexingSpiQueryTxSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheMultipleIndexedTypesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlQueryMinMaxTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCircularQueueTest.class));
        suite.addTest(new JUnit4TestAdapter(IndexingSpiQueryWithH2IndexingSelfTest.class));

        // DDL.
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexTransactionalReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexTransactionalPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexTransactionalPartitionedNearSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexAtomicReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexAtomicPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexAtomicPartitionedNearSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicTableSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicColumnsClientBasicSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicColumnsServerBasicSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicColumnsServerCoordinatorBasicSelfTest.class));

        // DML+DDL.
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexingComplexClientAtomicPartitionedTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexingComplexClientAtomicPartitionedNoBackupsTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexingComplexClientAtomicReplicatedTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexingComplexClientTransactionalPartitionedTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexingComplexClientTransactionalPartitionedNoBackupsTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexingComplexClientTransactionalReplicatedTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexingComplexServerAtomicPartitionedTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexingComplexServerAtomicPartitionedNoBackupsTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexingComplexServerAtomicReplicatedTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexingComplexServerTransactionalPartitionedTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexingComplexServerTransactionalPartitionedNoBackupsTest.class));
        suite.addTest(new JUnit4TestAdapter(H2DynamicIndexingComplexServerTransactionalReplicatedTest.class));

        suite.addTest(new JUnit4TestAdapter(DdlTransactionSelfTest.class));

        // Fields queries.
        suite.addTest(new JUnit4TestAdapter(SqlFieldsQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheLocalFieldsQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheReplicatedFieldsQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheReplicatedFieldsQueryROSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheReplicatedFieldsQueryJoinNoPrimaryPartitionsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedFieldsQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicFieldsQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNearEnabledFieldsQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedFieldsQueryP2PEnabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheFieldsQueryNoDataSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheQueryIndexingDisabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridOrderedMessageCancelSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheQueryEvictDataLostTest.class));

        // Full text queries.
        suite.addTest(new JUnit4TestAdapter(GridCacheFullTextQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheFullTextQueryNodeJoiningSelfTest.class));

        // Ignite cache and H2 comparison.
        suite.addTest(new JUnit4TestAdapter(BaseH2CompareQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(H2CompareBigQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(H2CompareBigQueryDistributedJoinsTest.class));

        // Cache query metrics.
        suite.addTest(new JUnit4TestAdapter(CacheLocalQueryMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CachePartitionedQueryMetricsDistributedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CachePartitionedQueryMetricsLocalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheReplicatedQueryMetricsDistributedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheReplicatedQueryMetricsLocalSelfTest.class));

        // Cache query metrics.
        suite.addTest(new JUnit4TestAdapter(CacheLocalQueryDetailMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CachePartitionedQueryDetailMetricsDistributedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CachePartitionedQueryDetailMetricsLocalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheReplicatedQueryDetailMetricsDistributedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheReplicatedQueryDetailMetricsLocalSelfTest.class));

        // Unmarshalling query test.
        suite.addTest(new JUnit4TestAdapter(IgniteCacheP2pUnmarshallingQueryErrorTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheNoClassQuerySelfTest.class));

        // Cancellation.
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDistributedQueryCancelSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheLocalQueryCancelOrTimeoutSelfTest.class));

        // Distributed joins.
        suite.addTest(new JUnit4TestAdapter(H2CompareBigQueryDistributedJoinsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDistributedJoinCollocatedAndNotTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDistributedJoinCustomAffinityMapper.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDistributedJoinNoIndexTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDistributedJoinPartitionedAndReplicatedTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDistributedJoinQueryConditionsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDistributedJoinTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlDistributedJoinSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlQueryParallelismTest.class));

        // Other.
        suite.addTest(new JUnit4TestAdapter(CacheIteratorScanQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheQueryNewClientSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheOffheapBatchIndexingSingleTypeTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheSqlQueryValueCopySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheQueryCacheDestroySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteQueryDedicatedPoolTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlEntryCacheModeAgnosticTest.class));
        suite.addTest(new JUnit4TestAdapter(QueryEntityCaseMismatchTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDistributedPartitionQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDistributedPartitionQueryNodeRestartsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheDistributedPartitionQueryConfigurationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlKeyValueFieldsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlRoutingTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlNotNullConstraintTest.class));
        suite.addTest(new JUnit4TestAdapter(LongIndexNameTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheQuerySqlFieldInlineSizeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlParameterizedQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(H2ConnectionLeaksSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCheckClusterStateBeforeExecuteQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(OptimizedMarshallerIndexNameTest.class));
        suite.addTest(new JUnit4TestAdapter(SqlSystemViewsSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridIndexRebuildSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(SqlTransactionCommandsWithMvccDisabledSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteSqlDefaultValueTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDecimalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSQLColumnConstraintsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTransactionSQLColumnConstraintTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedAtomicColumnConstraintsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedTransactionalColumnConstraintsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedTransactionalSnapshotColumnConstraintTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheReplicatedAtomicColumnConstraintsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheReplicatedTransactionalColumnConstraintsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheReplicatedTransactionalSnapshotColumnConstraintTest.class));

        // H2 Rows on-heap cache
        suite.addTest(new JUnit4TestAdapter(H2RowCacheSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2RowCachePageEvictionTest.class));

        // User operation SQL
        suite.addTest(new JUnit4TestAdapter(SqlParserUserSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SqlUserCommandSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(EncryptedSqlTableTest.class));

        suite.addTest(new JUnit4TestAdapter(ThreadLocalObjectPoolSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2StatementCacheSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(PreparedStatementExSelfTest.class));

        // Partition loss.
        suite.addTest(new JUnit4TestAdapter(IndexingCachePartitionLossPolicySelfTest.class));

        // GROUP_CONCAT
        suite.addTest(new JUnit4TestAdapter(IgniteSqlGroupConcatCollocatedTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSqlGroupConcatNotCollocatedTest.class));

        // Binary
        suite.addTest(new JUnit4TestAdapter(BinarySerializationQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BinarySerializationQueryWithReflectiveSerializerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheBinaryObjectsScanSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheBinaryObjectsScanWithEventsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BigEntryQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(BinaryMetadataConcurrentUpdateWithIndexesTest.class));

        // Partition pruning.
        suite.addTest(new JUnit4TestAdapter(InOperationExtractPartitionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(AndOperationExtractPartitionSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheDynamicLoadOnClientTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheDynamicLoadOnClientPersistentTest.class));

        return suite;
    }
}
