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

import org.apache.ignite.internal.processors.query.h2.RowCountTableStatisticsSurvivesNodeRestartTest;
import org.apache.ignite.internal.processors.query.h2.RowCountTableStatisticsUsageTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//    AffinityKeyNameAndValueFieldNameConflictTest.class,
//    DmlInsideTransactionTest.class,
//    ComplexPrimaryKeyUnwrapSelfTest.class,
//    SqlNestedQuerySelfTest.class,
//    ExplainSelfTest.class,
//    RunningQueriesTest.class,
//
//    PartitionedSqlTest.class,
//    ReplicatedSqlTest.class,
//
//    SqlParserCreateIndexSelfTest.class,
//    SqlParserDropIndexSelfTest.class,
//    SqlParserTransactionalKeywordsSelfTest.class,
//    SqlParserBulkLoadSelfTest.class,
//    SqlParserSetStreamingSelfTest.class,
//    SqlParserKillQuerySelfTest.class,
//    SqlParserMultiStatementSelfTest.class,
//
//    SqlConnectorConfigurationValidationSelfTest.class,
//    ClientConnectorConfigurationValidationSelfTest.class,
//
//    SqlSchemaSelfTest.class,
//    SqlIllegalSchemaSelfTest.class,
//    MultipleStatementsSqlQuerySelfTest.class,
//
//    BasicIndexTest.class,
//    BasicIndexMultinodeTest.class,
//
//    // Misc tests.
//    QueryEntityValidationSelfTest.class,
//    DuplicateKeyValueClassesSelfTest.class,
//    GridCacheLazyQueryPartitionsReleaseTest.class,
//
//    // Dynamic index create/drop tests.
//    SchemaExchangeSelfTest.class,
//
//    DynamicIndexServerCoordinatorBasicSelfTest.class,
//    DynamicIndexServerBasicSelfTest.class,
//    DynamicIndexServerNodeFilterCoordinatorBasicSelfTest.class,
//    DynamicIndexServerNodeFIlterBasicSelfTest.class,
//    DynamicIndexClientBasicSelfTest.class,
//
//    // Parsing
//    GridQueryParsingTest.class,
//    IgniteCacheSqlQueryErrorSelfTest.class,
//    IgniteCacheSqlDmlErrorSelfTest.class,
//    SqlUnsupportedSelfTest.class,
//
//    // Config.
//    IgniteCacheDuplicateEntityConfigurationSelfTest.class,
//    IncorrectQueryEntityTest.class,
//    IgniteDynamicSqlRestoreTest.class,
//
//    // Queries tests.
//    IgniteQueryTableLockAndConnectionPoolLazyModeOnTest.class,
//    IgniteQueryTableLockAndConnectionPoolLazyModeOffTest.class,
//    IgniteSqlSplitterSelfTest.class,
//    SqlPushDownFunctionTest.class,
//    IgniteSqlSegmentedIndexSelfTest.class,
//    IgniteCachelessQueriesSelfTest.class,
//    IgniteSqlSegmentedIndexMultiNodeSelfTest.class,
//    IgniteSqlSchemaIndexingTest.class,
//    GridCacheQueryIndexDisabledSelfTest.class,
//    IgniteCacheQueryLoadSelfTest.class,
//    IgniteCacheLocalQuerySelfTest.class,
//    IgniteCacheLocalAtomicQuerySelfTest.class,
//    IgniteCacheReplicatedQuerySelfTest.class,
//    IgniteCacheReplicatedQueryP2PDisabledSelfTest.class,
//    IgniteCacheReplicatedQueryEvtsDisabledSelfTest.class,
//    IgniteCachePartitionedQuerySelfTest.class,
//    IgniteCachePartitionedSnapshotEnabledQuerySelfTest.class,
//    IgniteCacheAtomicQuerySelfTest.class,
//    IgniteCacheAtomicNearEnabledQuerySelfTest.class,
//    IgniteCachePartitionedQueryP2PDisabledSelfTest.class,
//    IgniteCachePartitionedQueryEvtsDisabledSelfTest.class,
//
//    IgniteCacheUnionDuplicatesTest.class,
//    IgniteCacheJoinPartitionedAndReplicatedCollocationTest.class,
//    IgniteClientReconnectCacheQueriesFailoverTest.class,
//    IgniteErrorOnRebalanceTest.class,
//    CacheQueryBuildValueTest.class,
//    CacheOffheapBatchIndexingMultiTypeTest.class,
//
//    IgniteCacheQueryIndexSelfTest.class,
//    IgniteCacheCollocatedQuerySelfTest.class,
//    IgniteCacheLargeResultSelfTest.class,
//    GridCacheQueryInternalKeysSelfTest.class,
//    H2ResultSetIteratorNullifyOnEndSelfTest.class,
//    IgniteSqlBigIntegerKeyTest.class,
//    IgniteCacheOffheapEvictQueryTest.class,
//    IgniteCacheOffheapIndexScanTest.class,
//
//    GridCacheCrossCacheQuerySelfTest.class,
//    GridCacheQuerySerializationSelfTest.class,
//    IgniteBinaryObjectFieldsQuerySelfTest.class,
//    IgniteStableBaselineBinObjFieldsQuerySelfTest.class,
//    IgniteBinaryWrappedObjectFieldsQuerySelfTest.class,
//    IgniteCacheQueryH2IndexingLeakTest.class,
//    IgniteCacheQueryNoRebalanceSelfTest.class,
//    GridCacheQueryTransformerSelfTest.class,
//    CacheScanQueryFailoverTest.class,
//    IgniteCachePrimitiveFieldsQuerySelfTest.class,
//
//    IgniteCacheJoinQueryWithAffinityKeyTest.class,
//    IgniteCacheJoinPartitionedAndReplicatedTest.class,
//    IgniteCrossCachesJoinsQueryTest.class,
//
//    IgniteCacheMultipleIndexedTypesTest.class,
//    CacheDataPageScanQueryTest.class,
//    QueryDataPageScanTest.class,
//
//    // DML.
//    IgniteCacheMergeSqlQuerySelfTest.class,
//    IgniteCacheInsertSqlQuerySelfTest.class,
//    IgniteCacheUpdateSqlQuerySelfTest.class,
//    IgniteCacheDeleteSqlQuerySelfTest.class,
//    IgniteSqlSkipReducerOnUpdateDmlSelfTest.class,
//    IgniteSqlSkipReducerOnUpdateDmlFlagSelfTest.class,
//    IgniteCacheSqlInsertValidationSelfTest.class,
//
//    IgniteBinaryObjectQueryArgumentsTest.class,
//    IgniteBinaryObjectLocalQueryArgumentsTest.class,
//
//    IndexingSpiQuerySelfTest.class,
//    IndexingSpiQueryTxSelfTest.class,
//
//    IgniteCacheMultipleIndexedTypesTest.class,
//    IgniteSqlQueryMinMaxTest.class,
//
//    GridCircularQueueTest.class,
//    IndexingSpiQueryWithH2IndexingSelfTest.class,
//
//    // DDL.
//    H2DynamicIndexTransactionalReplicatedSelfTest.class,
//    H2DynamicIndexTransactionalPartitionedSelfTest.class,
//    H2DynamicIndexTransactionalPartitionedNearSelfTest.class,
//    H2DynamicIndexAtomicReplicatedSelfTest.class,
//    H2DynamicIndexAtomicPartitionedSelfTest.class,
//    H2DynamicIndexAtomicPartitionedNearSelfTest.class,
//    H2DynamicTableSelfTest.class,
//    H2DynamicColumnsClientBasicSelfTest.class,
//    H2DynamicColumnsServerBasicSelfTest.class,
//    H2DynamicColumnsServerCoordinatorBasicSelfTest.class,
//
//    // DML+DDL.
//    H2DynamicIndexingComplexClientAtomicPartitionedTest.class,
//    H2DynamicIndexingComplexClientAtomicPartitionedNoBackupsTest.class,
//    H2DynamicIndexingComplexClientAtomicReplicatedTest.class,
//    H2DynamicIndexingComplexClientTransactionalPartitionedTest.class,
//    H2DynamicIndexingComplexClientTransactionalPartitionedNoBackupsTest.class,
//    H2DynamicIndexingComplexClientTransactionalReplicatedTest.class,
//    H2DynamicIndexingComplexServerAtomicPartitionedTest.class,
//    H2DynamicIndexingComplexServerAtomicPartitionedNoBackupsTest.class,
//    H2DynamicIndexingComplexServerAtomicReplicatedTest.class,
//    H2DynamicIndexingComplexServerTransactionalPartitionedTest.class,
//    H2DynamicIndexingComplexServerTransactionalPartitionedNoBackupsTest.class,
//    H2DynamicIndexingComplexServerTransactionalReplicatedTest.class,
//
//    DdlTransactionSelfTest.class,
//
//    // Fields queries.
//    SqlFieldsQuerySelfTest.class,
//    IgniteCacheLocalFieldsQuerySelfTest.class,
//    IgniteCacheReplicatedFieldsQuerySelfTest.class,
//    IgniteCacheReplicatedFieldsQueryROSelfTest.class,
//    IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest.class,
//    IgniteCacheReplicatedFieldsQueryJoinNoPrimaryPartitionsSelfTest.class,
//    IgniteCachePartitionedFieldsQuerySelfTest.class,
//    IgniteCacheAtomicFieldsQuerySelfTest.class,
//    IgniteCacheAtomicNearEnabledFieldsQuerySelfTest.class,
//    IgniteCachePartitionedFieldsQueryP2PEnabledSelfTest.class,
//    IgniteCacheFieldsQueryNoDataSelfTest.class,
//    GridCacheQueryIndexingDisabledSelfTest.class,
//    GridOrderedMessageCancelSelfTest.class,
//    CacheQueryEvictDataLostTest.class,
//
//    // Full text queries.
//    GridCacheFullTextQuerySelfTest.class,
//    IgniteCacheFullTextQueryNodeJoiningSelfTest.class,
//
//    // Ignite cache and H2 comparison.
//    BaseH2CompareQueryTest.class,
//    H2CompareBigQueryTest.class,
//    H2CompareBigQueryDistributedJoinsTest.class,
//
//    // Cache query metrics.
//    CacheLocalQueryMetricsSelfTest.class,
//    CachePartitionedQueryMetricsDistributedSelfTest.class,
//    CachePartitionedQueryMetricsLocalSelfTest.class,
//    CacheReplicatedQueryMetricsDistributedSelfTest.class,
//    CacheReplicatedQueryMetricsLocalSelfTest.class,
//
//    // Cache query metrics.
//    CacheLocalQueryDetailMetricsSelfTest.class,
//    CachePartitionedQueryDetailMetricsDistributedSelfTest.class,
//    CachePartitionedQueryDetailMetricsLocalSelfTest.class,
//    CacheReplicatedQueryDetailMetricsDistributedSelfTest.class,
//    CacheReplicatedQueryDetailMetricsLocalSelfTest.class,
//
//    // Unmarshalling query test.
//    IgniteCacheP2pUnmarshallingQueryErrorTest.class,
//    IgniteCacheNoClassQuerySelfTest.class,
//
//    // Cancellation.
//    IgniteCacheDistributedQueryCancelSelfTest.class,
//    IgniteCacheLocalQueryCancelOrTimeoutSelfTest.class,
//
//    // Distributed joins.
//    H2CompareBigQueryDistributedJoinsTest.class,
//    IgniteCacheDistributedJoinCollocatedAndNotTest.class,
//    IgniteCacheDistributedJoinCustomAffinityMapper.class,
//    IgniteCacheDistributedJoinNoIndexTest.class,
//    IgniteCacheDistributedJoinPartitionedAndReplicatedTest.class,
//    IgniteCacheDistributedJoinQueryConditionsTest.class,
//    IgniteCacheDistributedJoinTest.class,
//    IgniteSqlDistributedJoinSelfTest.class,
//    IgniteSqlQueryParallelismTest.class,
//
//    // Other.
//    CacheIteratorScanQueryTest.class,
//    CacheQueryNewClientSelfTest.class,
//    CacheOffheapBatchIndexingSingleTypeTest.class,
//    CacheSqlQueryValueCopySelfTest.class,
//    IgniteCacheQueryCacheDestroySelfTest.class,
//    IgniteQueryDedicatedPoolTest.class,
//    IgniteSqlEntryCacheModeAgnosticTest.class,
//    QueryEntityCaseMismatchTest.class,
//    IgniteCacheDistributedPartitionQuerySelfTest.class,
//    IgniteCacheDistributedPartitionQueryNodeRestartsSelfTest.class,
//    IgniteCacheDistributedPartitionQueryConfigurationSelfTest.class,
//    IgniteSqlKeyValueFieldsTest.class,
//    IgniteSqlRoutingTest.class,
//    IgniteSqlNotNullConstraintTest.class,
//    LongIndexNameTest.class,
//    GridCacheQuerySqlFieldInlineSizeSelfTest.class,
//    IgniteSqlParameterizedQueryTest.class,
//    H2ConnectionLeaksSelfTest.class,
//    IgniteCheckClusterStateBeforeExecuteQueryTest.class,
//    OptimizedMarshallerIndexNameTest.class,
//    SqlSystemViewsSelfTest.class,
//
//    GridIndexRebuildSelfTest.class,
//    GridIndexFullRebuildTest.class,
//
//    SqlTransactionCommandsWithMvccDisabledSelfTest.class,
//
//    IgniteSqlDefaultValueTest.class,
//    IgniteDecimalSelfTest.class,
//    IgniteSQLColumnConstraintsTest.class,
//    IgniteTransactionSQLColumnConstraintTest.class,
//
//    IgniteCachePartitionedAtomicColumnConstraintsTest.class,
//    IgniteCachePartitionedTransactionalColumnConstraintsTest.class,
//    IgniteCachePartitionedTransactionalSnapshotColumnConstraintTest.class,
//    IgniteCacheReplicatedAtomicColumnConstraintsTest.class,
//    IgniteCacheReplicatedTransactionalColumnConstraintsTest.class,
//    IgniteCacheReplicatedTransactionalSnapshotColumnConstraintTest.class,
//
//    // H2 Rows on-heap cache
//    H2RowCacheSelfTest.class,
//    H2RowCachePageEvictionTest.class,
//    H2RowExpireTimeIndexSelfTest.class,
//
//    // User operation SQL
//    SqlParserUserSelfTest.class,
//    SqlUserCommandSelfTest.class,
//    EncryptedSqlTableTest.class,
//
//    ThreadLocalObjectPoolSelfTest.class,
//
//    // Partition loss.
//    IndexingCachePartitionLossPolicySelfTest.class,
//
//    // GROUP_CONCAT
//    IgniteSqlGroupConcatCollocatedTest.class,
//    IgniteSqlGroupConcatNotCollocatedTest.class,
//
//    // Binary
//    BinarySerializationQuerySelfTest.class,
//    BinarySerializationQueryWithReflectiveSerializerSelfTest.class,
//    IgniteCacheBinaryObjectsScanSelfTest.class,
//    IgniteCacheBinaryObjectsScanWithEventsSelfTest.class,
//    BigEntryQueryTest.class,
//    BinaryMetadataConcurrentUpdateWithIndexesTest.class,
//
//    // Partition pruning.
//    InOperationExtractPartitionSelfTest.class,
//    AndOperationExtractPartitionSelfTest.class,
//    BetweenOperationExtractPartitionSelfTest.class,
//    JoinPartitionPruningSelfTest.class,
//    DmlSelectPartitionPruningSelfTest.class,
//    MvccDmlPartitionPruningSelfTest.class,
//
//    GridCacheDynamicLoadOnClientTest.class,
//    GridCacheDynamicLoadOnClientPersistentTest.class,
//
//    SqlDataTypeConversionTest.class,
//    ParameterTypeInferenceTest.class,
//
//    //Query history.
//    SqlQueryHistorySelfTest.class,
//    SqlQueryHistoryFromClientSelfTest.class,
//
//    SqlIncompatibleDataTypeExceptionTest.class,
//
//    //Cancellation of queries.
//    KillQueryTest.class,
//    KillQueryFromNeighbourTest.class,
//    KillQueryFromClientTest.class,
//    KillQueryOnClientDisconnectTest.class,

    // Table statistics.
    RowCountTableStatisticsUsageTest.class,
    RowCountTableStatisticsSurvivesNodeRestartTest.class,

//    SqlViewExporterSpiTest.class

})
public class IgniteBinaryCacheQueryTestSuite {
}
