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

import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistryTest;
import org.apache.ignite.internal.cdc.CacheEventsCdcTest;
import org.apache.ignite.internal.cdc.CdcIndexRebuildTest;
import org.apache.ignite.internal.cdc.SqlCdcTest;
import org.apache.ignite.internal.metric.SystemViewSelfTest;
import org.apache.ignite.internal.processors.cache.BigEntryQueryTest;
import org.apache.ignite.internal.processors.cache.BinaryMetadataConcurrentUpdateWithIndexesTest;
import org.apache.ignite.internal.processors.cache.BinarySerializationQuerySelfTest;
import org.apache.ignite.internal.processors.cache.BinarySerializationQueryWithReflectiveSerializerSelfTest;
import org.apache.ignite.internal.processors.cache.CacheIteratorScanQueryTest;
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
import org.apache.ignite.internal.processors.cache.CheckIndexesInlineSizeOnNodeJoinMultiJvmTest;
import org.apache.ignite.internal.processors.cache.GridCacheDynamicLoadOnClientPersistentTest;
import org.apache.ignite.internal.processors.cache.GridCacheDynamicLoadOnClientTest;
import org.apache.ignite.internal.processors.cache.GridCacheFullTextQueryFailoverTest;
import org.apache.ignite.internal.processors.cache.GridCacheFullTextQueryLimitTest;
import org.apache.ignite.internal.processors.cache.GridCacheFullTextQueryMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheFullTextQueryPagesTest;
import org.apache.ignite.internal.processors.cache.GridCacheFullTextQuerySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryIndexingDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQuerySqlFieldInlineSizeSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheBinaryObjectsScanSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheBinaryObjectsScanWithEventsSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinCollocatedAndNotTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinCustomAffinityMapper;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinNoIndexTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinPartitionedAndReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinQueryConditionsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheFieldsQueryNoDataSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheFullTextQueryNodeJoiningSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheNoClassQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheP2pUnmarshallingQueryErrorTest;
import org.apache.ignite.internal.processors.cache.IgniteCheckClusterStateBeforeExecuteQueryTest;
import org.apache.ignite.internal.processors.cache.IndexingCachePartitionLossPolicySelfTest;
import org.apache.ignite.internal.processors.cache.QueryEntityCaseMismatchTest;
import org.apache.ignite.internal.processors.cache.ReservationsOnDoneAfterTopologyUnlockFailTest;
import org.apache.ignite.internal.processors.cache.SqlFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.authentication.SqlUserCommandSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicNearEnabledFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedPartitionQueryConfigurationSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedPartitionQueryNodeRestartsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedPartitionQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheDistributedQueryCancelSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedFieldsQueryP2PEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryJoinNoPrimaryPartitionsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQueryROSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.encryption.EncryptedSqlTableTest;
import org.apache.ignite.internal.processors.cache.encryption.EncryptedSqlTemplateTableTest;
import org.apache.ignite.internal.processors.cache.index.BasicJavaTypesIndexTest;
import org.apache.ignite.internal.processors.cache.index.BasicSqlTypesIndexTest;
import org.apache.ignite.internal.processors.cache.index.DateIndexKeyTypeTest;
import org.apache.ignite.internal.processors.cache.index.H2ConnectionLeaksSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2RowCachePageEvictionTest;
import org.apache.ignite.internal.processors.cache.index.H2RowCacheSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2RowExpireTimeIndexSelfTest;
import org.apache.ignite.internal.processors.cache.index.IgniteDecimalSelfTest;
import org.apache.ignite.internal.processors.cache.index.IndexColumnTypeMismatchTest;
import org.apache.ignite.internal.processors.cache.index.LongIndexNameTest;
import org.apache.ignite.internal.processors.cache.index.OptimizedMarshallerIndexNameTest;
import org.apache.ignite.internal.processors.cache.index.PojoIndexLocalQueryTest;
import org.apache.ignite.internal.processors.cache.index.SqlPartitionEvictionTest;
import org.apache.ignite.internal.processors.cache.index.SqlTransactionCommandsWithMvccDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.index.SqlTransactionsSelfTest;
import org.apache.ignite.internal.processors.cache.metric.SqlViewExporterSpiTest;
import org.apache.ignite.internal.processors.cache.query.IgniteCacheQueryCacheDestroySelfTest;
import org.apache.ignite.internal.processors.cache.query.ScanQueryConcurrentSqlUpdatesTest;
import org.apache.ignite.internal.processors.cache.query.ScanQueryConcurrentUpdatesTest;
import org.apache.ignite.internal.processors.query.IgniteQueryConvertibleTypesValidationTest;
import org.apache.ignite.internal.processors.query.IgniteQueryDedicatedPoolTest;
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
import org.apache.ignite.internal.processors.query.IgniteSqlQueryDecimalArgumentsWithTest;
import org.apache.ignite.internal.processors.query.IgniteSqlQueryParallelismTest;
import org.apache.ignite.internal.processors.query.IgniteSqlRoutingTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSchemasDiffConfigurationsTest;
import org.apache.ignite.internal.processors.query.JdbcSqlCustomSchemaTest;
import org.apache.ignite.internal.processors.query.JdbcSqlDefaultSchemaTest;
import org.apache.ignite.internal.processors.query.KillQueryErrorOnCancelTest;
import org.apache.ignite.internal.processors.query.KillQueryFromClientTest;
import org.apache.ignite.internal.processors.query.KillQueryFromNeighbourTest;
import org.apache.ignite.internal.processors.query.KillQueryOnClientDisconnectTest;
import org.apache.ignite.internal.processors.query.KillQueryTest;
import org.apache.ignite.internal.processors.query.RemoveConstantsFromQueryTest;
import org.apache.ignite.internal.processors.query.SqlFieldTypeValidationOnKeyValueInsertTest;
import org.apache.ignite.internal.processors.query.SqlFieldTypeValidationTypesTest;
import org.apache.ignite.internal.processors.query.SqlIncompatibleDataTypeExceptionTest;
import org.apache.ignite.internal.processors.query.SqlMergeOnClientNodeTest;
import org.apache.ignite.internal.processors.query.SqlMergeTest;
import org.apache.ignite.internal.processors.query.SqlNotNullKeyValueFieldTest;
import org.apache.ignite.internal.processors.query.SqlQueryHistoryFromClientSelfTest;
import org.apache.ignite.internal.processors.query.SqlQueryHistorySelfTest;
import org.apache.ignite.internal.processors.query.SqlQueryIndexWithDifferentTypeTest;
import org.apache.ignite.internal.processors.query.SqlSystemViewsSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridIndexRebuildSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridIndexRebuildTest;
import org.apache.ignite.internal.processors.query.h2.QueryParserMetricsHolderSelfTest;
import org.apache.ignite.internal.processors.query.h2.RowCountTableStatisticsSurvivesNodeRestartTest;
import org.apache.ignite.internal.processors.query.h2.RowCountTableStatisticsUsageTest;
import org.apache.ignite.internal.processors.query.h2.ThreadLocalObjectPoolSelfTest;
import org.apache.ignite.internal.processors.query.h2.sql.BaseH2CompareQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryDistributedJoinsTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.ParameterTypeInferenceTest;
import org.apache.ignite.internal.processors.query.h2.twostep.AndOperationExtractPartitionSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.BetweenOperationExtractPartitionSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.DmlSelectPartitionPruningSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.InOperationExtractPartitionSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.JoinPartitionPruningSelfTest;
import org.apache.ignite.internal.processors.query.h2.twostep.JoinQueryEntityPartitionPruningSelfTest;
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
import org.apache.ignite.internal.sql.SqlParserUserSelfTest;
import org.apache.ignite.spi.communication.tcp.GridOrderedMessageCancelSelfTest;
import org.apache.ignite.util.KillCommandsMXBeanTest;
import org.apache.ignite.util.KillCommandsSQLTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Fields queries.
    SqlFieldsQuerySelfTest.class,
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
    IgniteSqlQueryDecimalArgumentsWithTest.class,
    SqlQueryIndexWithDifferentTypeTest.class,

    // Full text queries.
    GridCacheFullTextQueryFailoverTest.class,
    GridCacheFullTextQuerySelfTest.class,
    GridCacheFullTextQueryMultithreadedSelfTest.class,
    GridCacheFullTextQueryPagesTest.class,
    GridCacheFullTextQueryLimitTest.class,
    IgniteCacheFullTextQueryNodeJoiningSelfTest.class,

    // Ignite cache and H2 comparison.
    BaseH2CompareQueryTest.class,
    H2CompareBigQueryTest.class,
    H2CompareBigQueryDistributedJoinsTest.class,
    IndexColumnTypeMismatchTest.class,

    // Cache query metrics.
    CachePartitionedQueryMetricsDistributedSelfTest.class,
    CachePartitionedQueryMetricsLocalSelfTest.class,
    CacheReplicatedQueryMetricsDistributedSelfTest.class,
    CacheReplicatedQueryMetricsLocalSelfTest.class,

    // Cache query metrics.
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
    ReservationsOnDoneAfterTopologyUnlockFailTest.class,

    GridIndexRebuildSelfTest.class,
    GridIndexRebuildTest.class,
    CheckIndexesInlineSizeOnNodeJoinMultiJvmTest.class,

    SqlTransactionCommandsWithMvccDisabledSelfTest.class,
    SqlTransactionsSelfTest.class,

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

    ThreadLocalObjectPoolSelfTest.class,

    // H2 Rows on-heap cache
    H2RowCacheSelfTest.class,
    H2RowCachePageEvictionTest.class,
    H2RowExpireTimeIndexSelfTest.class,

    // User operation SQL
    SqlParserUserSelfTest.class,
    SqlUserCommandSelfTest.class,
    EncryptedSqlTableTest.class,
    EncryptedSqlTemplateTableTest.class,

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
    JoinQueryEntityPartitionPruningSelfTest.class,
    DmlSelectPartitionPruningSelfTest.class,
    MvccDmlPartitionPruningSelfTest.class,

    GridCacheDynamicLoadOnClientTest.class,
    GridCacheDynamicLoadOnClientPersistentTest.class,

    SqlDataTypeConversionTest.class,
    ParameterTypeInferenceTest.class,

    //Query history.
    SqlQueryHistorySelfTest.class,
    SqlQueryHistoryFromClientSelfTest.class,
    RemoveConstantsFromQueryTest.class,

    SqlIncompatibleDataTypeExceptionTest.class,

    BasicSqlTypesIndexTest.class,
    BasicJavaTypesIndexTest.class,
    PojoIndexLocalQueryTest.class,
    DateIndexKeyTypeTest.class,

    //Cancellation of queries.
    KillQueryTest.class,
    KillQueryFromNeighbourTest.class,
    KillQueryFromClientTest.class,
    KillQueryOnClientDisconnectTest.class,
    KillQueryErrorOnCancelTest.class,
    KillCommandsMXBeanTest.class,
    KillCommandsSQLTest.class,

    // Table statistics.
    RowCountTableStatisticsUsageTest.class,
    RowCountTableStatisticsSurvivesNodeRestartTest.class,

    SqlViewExporterSpiTest.class,
    SystemViewSelfTest.class,

    SqlMergeTest.class,
    SqlMergeOnClientNodeTest.class,

    SqlFieldTypeValidationTypesTest.class,
    SqlFieldTypeValidationOnKeyValueInsertTest.class,

    InlineIndexKeyTypeRegistryTest.class,

    IgniteQueryConvertibleTypesValidationTest.class,

    IgniteStatisticsTestSuite.class,

    // CDC tests.
    SqlCdcTest.class,
    CacheEventsCdcTest.class,
    CdcIndexRebuildTest.class
})
public class IgniteBinaryCacheQueryTestSuite3 {
}
