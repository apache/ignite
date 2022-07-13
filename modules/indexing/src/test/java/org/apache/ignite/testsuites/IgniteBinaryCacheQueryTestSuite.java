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

import org.apache.ignite.internal.processors.cache.AffinityAliasKeyTest;
import org.apache.ignite.internal.processors.cache.AffinityKeyNameAndValueFieldNameConflictTest;
import org.apache.ignite.internal.processors.cache.CacheOffheapBatchIndexingMultiTypeTest;
import org.apache.ignite.internal.processors.cache.CacheQueryBuildValueTest;
import org.apache.ignite.internal.processors.cache.DdlTransactionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheCrossCacheQuerySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheLazyQueryPartitionsReleaseTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryIndexDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryInternalKeysSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQuerySerializationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectLocalQueryArgumentsTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectQueryArgumentsTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryWrappedObjectFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheCollocatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDeleteSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDuplicateEntityConfigurationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInsertSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinPartitionedAndReplicatedCollocationTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinPartitionedAndReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinQueryWithAffinityKeyTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLargeResultSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheMergeSqlQueryFailingTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheMergeSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheMultipleIndexedTypesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapEvictQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheParallelismQuerySortOrderTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePrimitiveFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryH2IndexingLeakTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryIndexSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryLoadSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlDmlErrorSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlInsertValidationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlQueryErrorSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheUnionDuplicatesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheUpdateSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteClientReconnectCacheQueriesFailoverTest;
import org.apache.ignite.internal.processors.cache.IgniteCrossCachesJoinsQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicEnableIndexingRestoreTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicSqlRestoreTest;
import org.apache.ignite.internal.processors.cache.IgniteErrorOnRebalanceTest;
import org.apache.ignite.internal.processors.cache.IncorrectQueryEntityTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicNearEnabledQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQueryEvtsDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedSnapshotEnabledQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNoRebalanceSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQueryEvtsDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.index.ArrayIndexTest;
import org.apache.ignite.internal.processors.cache.index.BasicIndexMultinodeTest;
import org.apache.ignite.internal.processors.cache.index.BasicIndexTest;
import org.apache.ignite.internal.processors.cache.index.ComplexPrimaryKeyUnwrapSelfTest;
import org.apache.ignite.internal.processors.cache.index.ComplexSecondaryKeyUnwrapSelfTest;
import org.apache.ignite.internal.processors.cache.index.DuplicateKeyValueClassesSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexClientBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerCoordinatorBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerNodeFIlterBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerNodeFilterCoordinatorBasicSelfTest;
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
import org.apache.ignite.internal.processors.cache.index.IndexMetricsTest;
import org.apache.ignite.internal.processors.cache.index.QueryEntityValidationSelfTest;
import org.apache.ignite.internal.processors.cache.index.SchemaExchangeSelfTest;
import org.apache.ignite.internal.processors.cache.index.StopNodeOnRebuildIndexFailureTest;
import org.apache.ignite.internal.processors.cache.query.CacheDataPageScanQueryTest;
import org.apache.ignite.internal.processors.cache.query.CacheScanQueryFailoverTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryTransformerSelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCircularQueueTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQueryTxSelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQueryWithH2IndexingSelfTest;
import org.apache.ignite.internal.processors.cache.transaction.DmlInsideTransactionTest;
import org.apache.ignite.internal.processors.client.ClientConnectorConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteStableBaselineBinObjFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.query.IgniteCachelessQueriesSelfTest;
import org.apache.ignite.internal.processors.query.IgniteQueryTableLockAndConnectionPoolLazyModeOffTest;
import org.apache.ignite.internal.processors.query.IgniteQueryTableLockAndConnectionPoolLazyModeOnTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSchemaIndexingTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSegmentedIndexMultiNodeSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSegmentedIndexSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSkipReducerOnUpdateDmlFlagSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSkipReducerOnUpdateDmlSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSplitterSelfTest;
import org.apache.ignite.internal.processors.query.MultipleStatementsSqlQuerySelfTest;
import org.apache.ignite.internal.processors.query.RunningQueriesTest;
import org.apache.ignite.internal.processors.query.SqlIllegalSchemaSelfTest;
import org.apache.ignite.internal.processors.query.SqlNestedQuerySelfTest;
import org.apache.ignite.internal.processors.query.SqlPushDownFunctionTest;
import org.apache.ignite.internal.processors.query.SqlResultSetMetaSelfTest;
import org.apache.ignite.internal.processors.query.SqlSchemaSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridSubqueryJoinOptimizerSelfTest;
import org.apache.ignite.internal.processors.query.h2.H2ResultSetIteratorNullifyOnEndSelfTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlBigIntegerKeyTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlQueryMinMaxTest;
import org.apache.ignite.internal.processors.query.h2.QueryDataPageScanTest;
import org.apache.ignite.internal.processors.query.h2.sql.ExplainSelfTest;
import org.apache.ignite.internal.processors.query.h2.sql.GridQueryParsingTest;
import org.apache.ignite.internal.processors.query.h2.sql.SqlUnsupportedSelfTest;
import org.apache.ignite.internal.processors.sql.SqlConnectorConfigurationValidationSelfTest;
import org.apache.ignite.internal.sql.SqlParserBulkLoadSelfTest;
import org.apache.ignite.internal.sql.SqlParserCreateIndexSelfTest;
import org.apache.ignite.internal.sql.SqlParserDropIndexSelfTest;
import org.apache.ignite.internal.sql.SqlParserKillQuerySelfTest;
import org.apache.ignite.internal.sql.SqlParserMultiStatementSelfTest;
import org.apache.ignite.internal.sql.SqlParserSetStreamingSelfTest;
import org.apache.ignite.internal.sql.SqlParserTransactionalKeywordsSelfTest;
import org.apache.ignite.sqltests.CheckWarnJoinPartitionedTables;
import org.apache.ignite.sqltests.PartitionedSqlTest;
import org.apache.ignite.sqltests.ReplicatedSqlCustomPartitionsTest;
import org.apache.ignite.sqltests.ReplicatedSqlTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    AffinityAliasKeyTest.class,
    AffinityKeyNameAndValueFieldNameConflictTest.class,
    DmlInsideTransactionTest.class,
    ComplexPrimaryKeyUnwrapSelfTest.class,
    ComplexSecondaryKeyUnwrapSelfTest.class,
    SqlNestedQuerySelfTest.class,
    ExplainSelfTest.class,
    RunningQueriesTest.class,

    PartitionedSqlTest.class,
    ReplicatedSqlTest.class,
    ReplicatedSqlCustomPartitionsTest.class,
    CheckWarnJoinPartitionedTables.class,
    DeletionDuringRebalanceTest.class,

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

    SqlResultSetMetaSelfTest.class,

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
    IgniteCacheReplicatedQuerySelfTest.class,
    IgniteCacheReplicatedQueryP2PDisabledSelfTest.class,
    IgniteCacheReplicatedQueryEvtsDisabledSelfTest.class,
    IgniteCachePartitionedQuerySelfTest.class,
    IgniteCachePartitionedSnapshotEnabledQuerySelfTest.class,
    IgniteCacheAtomicQuerySelfTest.class,
    IgniteCacheAtomicNearEnabledQuerySelfTest.class,
    IgniteCachePartitionedQueryP2PDisabledSelfTest.class,
    IgniteCachePartitionedQueryEvtsDisabledSelfTest.class,
    IgniteCacheParallelismQuerySortOrderTest.class,

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

    GridSubqueryJoinOptimizerSelfTest.class,

    // DML.
    IgniteCacheMergeSqlQuerySelfTest.class,
    IgniteCacheMergeSqlQueryFailingTest.class,
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

})
public class IgniteBinaryCacheQueryTestSuite {
}
