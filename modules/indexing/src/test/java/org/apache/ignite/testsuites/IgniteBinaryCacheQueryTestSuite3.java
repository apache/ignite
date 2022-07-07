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

import org.apache.ignite.internal.cdc.CacheEventsCdcTest;
import org.apache.ignite.internal.cdc.SqlCdcTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
/*
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
    IgniteSqlQueryDecimalArgumentsWithTest.class,

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
    H2ColumnTypeConversionCheckSelfTest.class,

    IgniteStatisticsTestSuite.class,
*/

    // CDC tests.
    SqlCdcTest.class,
    CacheEventsCdcTest.class

})
public class IgniteBinaryCacheQueryTestSuite3 {
}
