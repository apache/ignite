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

import org.apache.ignite.internal.metric.SystemViewSelfTest;
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
import org.apache.ignite.internal.processors.cache.CheckIndexesInlineSizeOnNodeJoinMultiJvmTest;
import org.apache.ignite.internal.processors.cache.DdlTransactionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheCrossCacheQuerySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheDynamicLoadOnClientPersistentTest;
import org.apache.ignite.internal.processors.cache.GridCacheDynamicLoadOnClientTest;
import org.apache.ignite.internal.processors.cache.GridCacheFullTextQueryMultithreadedSelfTest;
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
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlDmlErrorSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlInsertValidationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlQueryErrorSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheUnionDuplicatesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheUpdateSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCheckClusterStateBeforeExecuteQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteClientReconnectCacheQueriesFailoverTest;
import org.apache.ignite.internal.processors.cache.IgniteCrossCachesJoinsQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicEnableIndexingRestoreTest;
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
import org.apache.ignite.internal.processors.cache.index.ArrayIndexTest;
import org.apache.ignite.internal.processors.cache.index.BasicIndexMultinodeTest;
import org.apache.ignite.internal.processors.cache.index.BasicIndexTest;
import org.apache.ignite.internal.processors.cache.index.BasicJavaTypesIndexTest;
import org.apache.ignite.internal.processors.cache.index.BasicSqlTypesIndexTest;
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
import org.apache.ignite.internal.processors.cache.index.H2RowExpireTimeIndexSelfTest;
import org.apache.ignite.internal.processors.cache.index.IgniteDecimalSelfTest;
import org.apache.ignite.internal.processors.cache.index.IndexMetricsTest;
import org.apache.ignite.internal.processors.cache.index.LongIndexNameTest;
import org.apache.ignite.internal.processors.cache.index.OptimizedMarshallerIndexNameTest;
import org.apache.ignite.internal.processors.cache.index.QueryEntityValidationSelfTest;
import org.apache.ignite.internal.processors.cache.index.SchemaExchangeSelfTest;
import org.apache.ignite.internal.processors.cache.index.SqlPartitionEvictionTest;
import org.apache.ignite.internal.processors.cache.index.SqlTransactionCommandsWithMvccDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.index.StopNodeOnRebuildIndexFailureTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQueryCancelOrTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQuerySelfTest;
import org.apache.ignite.internal.processors.cache.metric.SqlViewExporterSpiTest;
import org.apache.ignite.internal.processors.cache.query.CacheDataPageScanQueryTest;
import org.apache.ignite.internal.processors.cache.query.CacheScanQueryFailoverTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryTransformerSelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCircularQueueTest;
import org.apache.ignite.internal.processors.cache.query.IgniteCacheQueryCacheDestroySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQueryTxSelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQueryWithH2IndexingSelfTest;
import org.apache.ignite.internal.processors.cache.query.ScanQueryConcurrentSqlUpdatesTest;
import org.apache.ignite.internal.processors.cache.query.ScanQueryConcurrentUpdatesTest;
import org.apache.ignite.internal.processors.cache.transaction.DmlInsideTransactionTest;
import org.apache.ignite.internal.processors.client.ClientConnectorConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteStableBaselineBinObjFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.query.IgniteCachelessQueriesSelfTest;
import org.apache.ignite.internal.processors.query.IgniteQueryDedicatedPoolTest;
import org.apache.ignite.internal.processors.query.IgniteQueryTableLockAndConnectionPoolLazyModeOffTest;
import org.apache.ignite.internal.processors.query.IgniteQueryTableLockAndConnectionPoolLazyModeOnTest;
import org.apache.ignite.internal.processors.query.IgniteSqlCustomSchemaTest;
import org.apache.ignite.internal.processors.query.IgniteSqlCustomSchemaWithPdsEnabled;
import org.apache.ignite.internal.processors.query.IgniteSqlDefaultSchemaTest;
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
import org.apache.ignite.internal.processors.query.IgniteSqlSchemasDiffConfigurationsTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSegmentedIndexMultiNodeSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSegmentedIndexSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSkipReducerOnUpdateDmlFlagSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSkipReducerOnUpdateDmlSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSplitterSelfTest;
import org.apache.ignite.internal.processors.query.JdbcSqlCustomSchemaTest;
import org.apache.ignite.internal.processors.query.JdbcSqlDefaultSchemaTest;
import org.apache.ignite.internal.processors.query.KillQueryErrorOnCancelTest;
import org.apache.ignite.internal.processors.query.KillQueryFromClientTest;
import org.apache.ignite.internal.processors.query.KillQueryFromNeighbourTest;
import org.apache.ignite.internal.processors.query.KillQueryOnClientDisconnectTest;
import org.apache.ignite.internal.processors.query.KillQueryTest;
import org.apache.ignite.internal.processors.query.MultipleStatementsSqlQuerySelfTest;
import org.apache.ignite.internal.processors.query.RunningQueriesTest;
import org.apache.ignite.internal.processors.query.SqlFieldTypeValidationOnKeyValueInsertTest;
import org.apache.ignite.internal.processors.query.SqlFieldTypeValidationTypesTest;
import org.apache.ignite.internal.processors.query.SqlIllegalSchemaSelfTest;
import org.apache.ignite.internal.processors.query.SqlIncompatibleDataTypeExceptionTest;
import org.apache.ignite.internal.processors.query.SqlMergeOnClientNodeTest;
import org.apache.ignite.internal.processors.query.SqlMergeTest;
import org.apache.ignite.internal.processors.query.SqlNestedQuerySelfTest;
import org.apache.ignite.internal.processors.query.SqlNotNullKeyValueFieldTest;
import org.apache.ignite.internal.processors.query.SqlPushDownFunctionTest;
import org.apache.ignite.internal.processors.query.SqlQueryHistoryFromClientSelfTest;
import org.apache.ignite.internal.processors.query.SqlQueryHistorySelfTest;
import org.apache.ignite.internal.processors.query.SqlSchemaSelfTest;
import org.apache.ignite.internal.processors.query.SqlSystemViewsSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridIndexRebuildSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridIndexRebuildTest;
import org.apache.ignite.internal.processors.query.h2.H2ResultSetIteratorNullifyOnEndSelfTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlBigIntegerKeyTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlQueryMinMaxTest;
import org.apache.ignite.internal.processors.query.h2.QueryDataPageScanTest;
import org.apache.ignite.internal.processors.query.h2.QueryParserMetricsHolderSelfTest;
import org.apache.ignite.internal.processors.query.h2.RowCountTableStatisticsSurvivesNodeRestartTest;
import org.apache.ignite.internal.processors.query.h2.RowCountTableStatisticsUsageTest;
import org.apache.ignite.internal.processors.query.h2.sql.BaseH2CompareQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.ExplainSelfTest;
import org.apache.ignite.internal.processors.query.h2.sql.GridQueryParsingTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryDistributedJoinsTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.ParameterTypeInferenceTest;
import org.apache.ignite.internal.processors.query.h2.sql.SqlUnsupportedSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.AndOperationExtractPartitionSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.BetweenOperationExtractPartitionSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DmlSelectPartitionPruningSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.InOperationExtractPartitionSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.JoinPartitionPruningSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.MvccDmlPartitionPruningSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.SqlDataTypeConversionTest;
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
import org.apache.ignite.internal.sql.SqlParserKillQuerySelfTest;
import org.apache.ignite.internal.sql.SqlParserMultiStatementSelfTest;
import org.apache.ignite.internal.sql.SqlParserSetStreamingSelfTest;
import org.apache.ignite.internal.sql.SqlParserTransactionalKeywordsSelfTest;
import org.apache.ignite.internal.sql.SqlParserUserSelfTest;
import org.apache.ignite.spi.communication.tcp.GridOrderedMessageCancelSelfTest;
import org.apache.ignite.sqltests.PartitionedSqlTest;
import org.apache.ignite.sqltests.ReplicatedSqlTest;
import org.apache.ignite.util.KillCommandsCommandShTest;
import org.apache.ignite.util.KillCommandsMXBeanTest;
import org.apache.ignite.util.KillCommandsSQLTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    AffinityKeyNameAndValueFieldNameConflictTest.class,
    DmlInsideTransactionTest.class,
    ComplexPrimaryKeyUnwrapSelfTest.class,
    SqlNestedQuerySelfTest.class,
    ExplainSelfTest.class,
    RunningQueriesTest.class,

    PartitionedSqlTest.class,
    ReplicatedSqlTest.class,

    SqlParserCreateIndexSelfTest.class,
    SqlParserDropIndexSelfTest.class,
    SqlParserTransactionalKeywordsSelfTest.class,
    SqlParserBulkLoadSelfTest.class,
    SqlParserSetStreamingSelfTest.class,
    SqlParserKillQuerySelfTest.class,
    SqlParserMultiStatementSelfTest.class,

    SqlConnectorConfigurationValidationSelfTest.class,
    ClientConnectorConfigurationValidationSelfTest.class,

    SqlSchemaSelfTest.class,
    SqlIllegalSchemaSelfTest.class,
    MultipleStatementsSqlQuerySelfTest.class,

    BasicIndexTest.class,
    ArrayIndexTest.class,
    BasicIndexMultinodeTest.class,
    IndexMetricsTest.class,

    // Misc tests.
    QueryEntityValidationSelfTest.class,
    DuplicateKeyValueClassesSelfTest.class,
    GridCacheLazyQueryPartitionsReleaseTest.class,
    StopNodeOnRebuildIndexFailureTest.class,

    // Dynamic index create/drop tests.
    SchemaExchangeSelfTest.class,

    DynamicIndexServerCoordinatorBasicSelfTest.class,
    DynamicIndexServerBasicSelfTest.class,
    DynamicIndexServerNodeFilterCoordinatorBasicSelfTest.class,
    DynamicIndexServerNodeFIlterBasicSelfTest.class,
    DynamicIndexClientBasicSelfTest.class,

    // Parsing
    GridQueryParsingTest.class,
    IgniteCacheSqlQueryErrorSelfTest.class,
    IgniteCacheSqlDmlErrorSelfTest.class,
    SqlUnsupportedSelfTest.class,

    // Config.
    IgniteCacheDuplicateEntityConfigurationSelfTest.class,
    IncorrectQueryEntityTest.class,
    IgniteDynamicSqlRestoreTest.class,
    IgniteDynamicEnableIndexingRestoreTest.class,

    // Queries tests.
    IgniteQueryTableLockAndConnectionPoolLazyModeOnTest.class,
    IgniteQueryTableLockAndConnectionPoolLazyModeOffTest.class,
    IgniteSqlSplitterSelfTest.class,
    SqlPushDownFunctionTest.class,
    IgniteSqlSegmentedIndexSelfTest.class,
    IgniteCachelessQueriesSelfTest.class,
    IgniteSqlSegmentedIndexMultiNodeSelfTest.class,
    IgniteSqlSchemaIndexingTest.class,
    GridCacheQueryIndexDisabledSelfTest.class,
    IgniteCacheQueryLoadSelfTest.class,
    IgniteCacheLocalQuerySelfTest.class,
    IgniteCacheLocalAtomicQuerySelfTest.class,
    IgniteCacheReplicatedQuerySelfTest.class,
    IgniteCacheReplicatedQueryP2PDisabledSelfTest.class,
    IgniteCacheReplicatedQueryEvtsDisabledSelfTest.class,
    IgniteCachePartitionedQuerySelfTest.class,
    IgniteCachePartitionedSnapshotEnabledQuerySelfTest.class,
    IgniteCacheAtomicQuerySelfTest.class,
    IgniteCacheAtomicNearEnabledQuerySelfTest.class,
    IgniteCachePartitionedQueryP2PDisabledSelfTest.class,
    IgniteCachePartitionedQueryEvtsDisabledSelfTest.class,

    IgniteCacheUnionDuplicatesTest.class,
    IgniteCacheJoinPartitionedAndReplicatedCollocationTest.class,
    IgniteClientReconnectCacheQueriesFailoverTest.class,
    IgniteErrorOnRebalanceTest.class,
    CacheQueryBuildValueTest.class,
    CacheOffheapBatchIndexingMultiTypeTest.class,

    IgniteCacheQueryIndexSelfTest.class,
    IgniteCacheCollocatedQuerySelfTest.class,
    IgniteCacheLargeResultSelfTest.class,
    GridCacheQueryInternalKeysSelfTest.class,
    H2ResultSetIteratorNullifyOnEndSelfTest.class,
    IgniteSqlBigIntegerKeyTest.class,
    IgniteCacheOffheapEvictQueryTest.class,
    IgniteCacheOffheapIndexScanTest.class,

    GridCacheCrossCacheQuerySelfTest.class,
    GridCacheQuerySerializationSelfTest.class,
    IgniteBinaryObjectFieldsQuerySelfTest.class,
    IgniteStableBaselineBinObjFieldsQuerySelfTest.class,
    IgniteBinaryWrappedObjectFieldsQuerySelfTest.class,
    IgniteCacheQueryH2IndexingLeakTest.class,
    IgniteCacheQueryNoRebalanceSelfTest.class,
    GridCacheQueryTransformerSelfTest.class,
    CacheScanQueryFailoverTest.class,
    IgniteCachePrimitiveFieldsQuerySelfTest.class,

    IgniteCacheJoinQueryWithAffinityKeyTest.class,
    IgniteCacheJoinPartitionedAndReplicatedTest.class,
    IgniteCrossCachesJoinsQueryTest.class,

    IgniteCacheMultipleIndexedTypesTest.class,
    CacheDataPageScanQueryTest.class,
    QueryDataPageScanTest.class,

    // DML.
    IgniteCacheMergeSqlQuerySelfTest.class,
    IgniteCacheInsertSqlQuerySelfTest.class,
    IgniteCacheUpdateSqlQuerySelfTest.class,
    IgniteCacheDeleteSqlQuerySelfTest.class,
    IgniteSqlSkipReducerOnUpdateDmlSelfTest.class,
    IgniteSqlSkipReducerOnUpdateDmlFlagSelfTest.class,
    IgniteCacheSqlInsertValidationSelfTest.class,

    IgniteBinaryObjectQueryArgumentsTest.class,
    IgniteBinaryObjectLocalQueryArgumentsTest.class,

    IndexingSpiQuerySelfTest.class,
    IndexingSpiQueryTxSelfTest.class,

    IgniteCacheMultipleIndexedTypesTest.class,
    IgniteSqlQueryMinMaxTest.class,

    GridCircularQueueTest.class,
    IndexingSpiQueryWithH2IndexingSelfTest.class,

    // DDL.
    H2DynamicIndexTransactionalReplicatedSelfTest.class,
    H2DynamicIndexTransactionalPartitionedSelfTest.class,
    H2DynamicIndexTransactionalPartitionedNearSelfTest.class,
    H2DynamicIndexAtomicReplicatedSelfTest.class,
    H2DynamicIndexAtomicPartitionedSelfTest.class,
    H2DynamicIndexAtomicPartitionedNearSelfTest.class,
    H2DynamicTableSelfTest.class,
    H2DynamicColumnsClientBasicSelfTest.class,
    H2DynamicColumnsServerBasicSelfTest.class,
    H2DynamicColumnsServerCoordinatorBasicSelfTest.class,

    // DML+DDL.
    H2DynamicIndexingComplexClientAtomicPartitionedTest.class,
    H2DynamicIndexingComplexClientAtomicPartitionedNoBackupsTest.class,
    H2DynamicIndexingComplexClientAtomicReplicatedTest.class,
    H2DynamicIndexingComplexClientTransactionalPartitionedTest.class,
    H2DynamicIndexingComplexClientTransactionalPartitionedNoBackupsTest.class,
    H2DynamicIndexingComplexClientTransactionalReplicatedTest.class,
    H2DynamicIndexingComplexServerAtomicPartitionedTest.class,
    H2DynamicIndexingComplexServerAtomicPartitionedNoBackupsTest.class,
    H2DynamicIndexingComplexServerAtomicReplicatedTest.class,
    H2DynamicIndexingComplexServerTransactionalPartitionedTest.class,
    H2DynamicIndexingComplexServerTransactionalPartitionedNoBackupsTest.class,
    H2DynamicIndexingComplexServerTransactionalReplicatedTest.class,

    DdlTransactionSelfTest.class,

    // Fields queries.
    SqlFieldsQuerySelfTest.class,
    IgniteCacheLocalFieldsQuerySelfTest.class,
    IgniteCacheReplicatedFieldsQuerySelfTest.class,
    IgniteCacheReplicatedFieldsQueryROSelfTest.class,
    IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest.class,
    IgniteCacheReplicatedFieldsQueryJoinNoPrimaryPartitionsSelfTest.class,
    IgniteCachePartitionedFieldsQuerySelfTest.class,
    IgniteCacheAtomicFieldsQuerySelfTest.class,
    IgniteCacheAtomicNearEnabledFieldsQuerySelfTest.class,
    IgniteCachePartitionedFieldsQueryP2PEnabledSelfTest.class,
    IgniteCacheFieldsQueryNoDataSelfTest.class,
    GridCacheQueryIndexingDisabledSelfTest.class,
    GridOrderedMessageCancelSelfTest.class,
    CacheQueryEvictDataLostTest.class,

    // Full text queries.
    GridCacheFullTextQuerySelfTest.class,
    GridCacheFullTextQueryMultithreadedSelfTest.class,
    IgniteCacheFullTextQueryNodeJoiningSelfTest.class,

    // Ignite cache and H2 comparison.
    BaseH2CompareQueryTest.class,
    H2CompareBigQueryTest.class,
    H2CompareBigQueryDistributedJoinsTest.class,

    // Cache query metrics.
    CacheLocalQueryMetricsSelfTest.class,
    CachePartitionedQueryMetricsDistributedSelfTest.class,
    CachePartitionedQueryMetricsLocalSelfTest.class,
    CacheReplicatedQueryMetricsDistributedSelfTest.class,
    CacheReplicatedQueryMetricsLocalSelfTest.class,

    // Cache query metrics.
    CacheLocalQueryDetailMetricsSelfTest.class,
    CachePartitionedQueryDetailMetricsDistributedSelfTest.class,
    CachePartitionedQueryDetailMetricsLocalSelfTest.class,
    CacheReplicatedQueryDetailMetricsDistributedSelfTest.class,
    CacheReplicatedQueryDetailMetricsLocalSelfTest.class,

    QueryParserMetricsHolderSelfTest.class,

    // Unmarshalling query test.
    IgniteCacheP2pUnmarshallingQueryErrorTest.class,
    IgniteCacheNoClassQuerySelfTest.class,

    // Cancellation.
    IgniteCacheDistributedQueryCancelSelfTest.class,
    IgniteCacheLocalQueryCancelOrTimeoutSelfTest.class,

    // Distributed joins.
    H2CompareBigQueryDistributedJoinsTest.class,
    IgniteCacheDistributedJoinCollocatedAndNotTest.class,
    IgniteCacheDistributedJoinCustomAffinityMapper.class,
    IgniteCacheDistributedJoinNoIndexTest.class,
    IgniteCacheDistributedJoinPartitionedAndReplicatedTest.class,
    IgniteCacheDistributedJoinQueryConditionsTest.class,
    IgniteCacheDistributedJoinTest.class,
    IgniteSqlDistributedJoinSelfTest.class,
    IgniteSqlQueryParallelismTest.class,

    // Other.
    CacheIteratorScanQueryTest.class,
    CacheQueryNewClientSelfTest.class,
    CacheOffheapBatchIndexingSingleTypeTest.class,
    CacheSqlQueryValueCopySelfTest.class,
    IgniteCacheQueryCacheDestroySelfTest.class,
    IgniteQueryDedicatedPoolTest.class,
    IgniteSqlEntryCacheModeAgnosticTest.class,
    QueryEntityCaseMismatchTest.class,
    IgniteCacheDistributedPartitionQuerySelfTest.class,
    IgniteCacheDistributedPartitionQueryNodeRestartsSelfTest.class,
    IgniteCacheDistributedPartitionQueryConfigurationSelfTest.class,
    IgniteSqlKeyValueFieldsTest.class,
    SqlNotNullKeyValueFieldTest.class,
    IgniteSqlRoutingTest.class,
    IgniteSqlNotNullConstraintTest.class,
    LongIndexNameTest.class,
    GridCacheQuerySqlFieldInlineSizeSelfTest.class,
    IgniteSqlParameterizedQueryTest.class,
    H2ConnectionLeaksSelfTest.class,
    IgniteCheckClusterStateBeforeExecuteQueryTest.class,
    OptimizedMarshallerIndexNameTest.class,
    SqlSystemViewsSelfTest.class,
    ScanQueryConcurrentUpdatesTest.class,
    ScanQueryConcurrentSqlUpdatesTest.class,

    GridIndexRebuildSelfTest.class,
    GridIndexRebuildTest.class,
    CheckIndexesInlineSizeOnNodeJoinMultiJvmTest.class,

    SqlTransactionCommandsWithMvccDisabledSelfTest.class,

    IgniteSqlDefaultValueTest.class,
    IgniteDecimalSelfTest.class,
    IgniteSQLColumnConstraintsTest.class,
    IgniteTransactionSQLColumnConstraintTest.class,

    IgniteSqlDefaultSchemaTest.class,
    IgniteSqlCustomSchemaTest.class,
    JdbcSqlDefaultSchemaTest.class,
    JdbcSqlCustomSchemaTest.class,
    IgniteSqlCustomSchemaWithPdsEnabled.class,
    IgniteSqlSchemasDiffConfigurationsTest.class,

    IgniteCachePartitionedAtomicColumnConstraintsTest.class,
    IgniteCachePartitionedTransactionalColumnConstraintsTest.class,
    IgniteCachePartitionedTransactionalSnapshotColumnConstraintTest.class,
    IgniteCacheReplicatedAtomicColumnConstraintsTest.class,
    IgniteCacheReplicatedTransactionalColumnConstraintsTest.class,
    IgniteCacheReplicatedTransactionalSnapshotColumnConstraintTest.class,

    // H2 Rows on-heap cache
    H2RowCacheSelfTest.class,
    H2RowCachePageEvictionTest.class,
    H2RowExpireTimeIndexSelfTest.class,

    // User operation SQL
    SqlParserUserSelfTest.class,
    SqlUserCommandSelfTest.class,
    EncryptedSqlTableTest.class,

    // Partition loss.
    IndexingCachePartitionLossPolicySelfTest.class,

    // Partitions eviction
    SqlPartitionEvictionTest.class,

    // GROUP_CONCAT
    IgniteSqlGroupConcatCollocatedTest.class,
    IgniteSqlGroupConcatNotCollocatedTest.class,

    // Binary
    BinarySerializationQuerySelfTest.class,
    BinarySerializationQueryWithReflectiveSerializerSelfTest.class,
    IgniteCacheBinaryObjectsScanSelfTest.class,
    IgniteCacheBinaryObjectsScanWithEventsSelfTest.class,
    BigEntryQueryTest.class,
    BinaryMetadataConcurrentUpdateWithIndexesTest.class,

    // Partition pruning.
    InOperationExtractPartitionSelfTest.class,
    AndOperationExtractPartitionSelfTest.class,
    BetweenOperationExtractPartitionSelfTest.class,
    JoinPartitionPruningSelfTest.class,
    DmlSelectPartitionPruningSelfTest.class,
    MvccDmlPartitionPruningSelfTest.class,

    GridCacheDynamicLoadOnClientTest.class,
    GridCacheDynamicLoadOnClientPersistentTest.class,

    SqlDataTypeConversionTest.class,
    ParameterTypeInferenceTest.class,

    //Query history.
    SqlQueryHistorySelfTest.class,
    SqlQueryHistoryFromClientSelfTest.class,

    SqlIncompatibleDataTypeExceptionTest.class,

    BasicSqlTypesIndexTest.class,
    BasicJavaTypesIndexTest.class,

    //Cancellation of queries.
    KillQueryTest.class,
    KillQueryFromNeighbourTest.class,
    KillQueryFromClientTest.class,
    KillQueryOnClientDisconnectTest.class,
    KillQueryErrorOnCancelTest.class,
    KillCommandsMXBeanTest.class,
    KillCommandsCommandShTest.class,
    KillCommandsSQLTest.class,

    // Table statistics.
    RowCountTableStatisticsUsageTest.class,
    RowCountTableStatisticsSurvivesNodeRestartTest.class,

    SqlViewExporterSpiTest.class,
    SystemViewSelfTest.class,

    IgniteCacheMergeSqlQuerySelfTest.class,
    SqlMergeTest.class,
    SqlMergeOnClientNodeTest.class,

    SqlFieldTypeValidationTypesTest.class,
    SqlFieldTypeValidationOnKeyValueInsertTest.class
})
public class IgniteBinaryCacheQueryTestSuite {
}
