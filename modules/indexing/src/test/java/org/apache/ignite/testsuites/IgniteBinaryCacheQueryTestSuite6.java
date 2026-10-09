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
import org.apache.ignite.internal.processors.cache.CacheOffheapBatchIndexingMultiTypeTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryIndexDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryInternalKeysSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryPartitionsReleaseTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDeleteSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDuplicateEntityConfigurationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinPartitionedAndReplicatedCollocationTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinQueryWithAffinityKeyTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapEvictQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheParallelismQuerySortOrderTest;
import org.apache.ignite.internal.processors.cache.IgniteCachePrimitiveFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlDmlErrorSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlInsertValidationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSqlQueryErrorSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteClientReconnectCacheQueriesFailoverTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicSqlRestoreTest;
import org.apache.ignite.internal.processors.cache.IncorrectQueryEntityTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicNearEnabledQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedSnapshotEnabledQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNoRebalanceSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQueryP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.index.BPlusTreeMetricsTest;
import org.apache.ignite.internal.processors.cache.index.BasicIndexMultinodeTest;
import org.apache.ignite.internal.processors.cache.index.ComplexPrimaryKeyUnwrapSelfTest;
import org.apache.ignite.internal.processors.cache.index.DuplicateKeyValueClassesSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexClientBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerCoordinatorBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerNodeFilterCoordinatorBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.ErroneousQueryEntityConfigurationTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicColumnsServerBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexAtomicPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexTransactionalPartitionedNearSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientAtomicPartitionedNoBackupsTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientAtomicPartitionedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientTransactionalPartitionedNoBackupsTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerAtomicPartitionedNoBackupsTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerAtomicPartitionedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerAtomicReplicatedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerTransactionalPartitionedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerTransactionalReplicatedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicTableSelfTest;
import org.apache.ignite.internal.processors.cache.index.IndexMetricsTest;
import org.apache.ignite.internal.processors.cache.index.QueryEntityValidationSelfTest;
import org.apache.ignite.internal.processors.cache.index.SchemaExchangeSelfTest;
import org.apache.ignite.internal.processors.cache.index.StopNodeOnRebuildIndexFailureTest;
import org.apache.ignite.internal.processors.cache.query.CacheScanQueryFailoverTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryTransformerSelfTest;
import org.apache.ignite.internal.processors.cache.transaction.DmlInsideTransactionTest;
import org.apache.ignite.internal.processors.client.ClientConnectorConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSegmentedIndexSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSkipReducerOnUpdateDmlFlagSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSkipReducerOnUpdateDmlSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSplitterSelfTest;
import org.apache.ignite.internal.processors.query.RunningQueriesTest;
import org.apache.ignite.internal.processors.query.SqlResultSetMetaSelfTest;
import org.apache.ignite.internal.processors.query.SqlSchemaSelfTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlBigIntegerKeyTest;
import org.apache.ignite.internal.processors.query.h2.sql.SqlUnsupportedSelfTest;
import org.apache.ignite.internal.sql.SqlParserCreateIndexSelfTest;
import org.apache.ignite.internal.sql.SqlParserKillQuerySelfTest;
import org.apache.ignite.internal.sql.SqlParserSetStreamingSelfTest;
import org.apache.ignite.internal.sql.SqlParserViewSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Split off from {@link IgniteBinaryCacheQueryTestSuite} to reduce the single-suite runtime in CI;
 * contains an independent subset of the same test classes.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    AffinityAliasKeyTest.class,
    DmlInsideTransactionTest.class,
    ComplexPrimaryKeyUnwrapSelfTest.class,
    RunningQueriesTest.class,
    DeletionDuringRebalanceTest.class,
    SqlParserCreateIndexSelfTest.class,
    SqlParserSetStreamingSelfTest.class,
    SqlParserKillQuerySelfTest.class,
    SqlParserViewSelfTest.class,
    ClientConnectorConfigurationValidationSelfTest.class,
    SqlSchemaSelfTest.class,
    SqlResultSetMetaSelfTest.class,
    ErroneousQueryEntityConfigurationTest.class,
    BasicIndexMultinodeTest.class,
    IndexMetricsTest.class,
    BPlusTreeMetricsTest.class,
    QueryEntityValidationSelfTest.class,
    DuplicateKeyValueClassesSelfTest.class,
    GridCacheQueryPartitionsReleaseTest.class,
    StopNodeOnRebuildIndexFailureTest.class,
    SchemaExchangeSelfTest.class,
    DynamicIndexServerCoordinatorBasicSelfTest.class,
    DynamicIndexServerNodeFilterCoordinatorBasicSelfTest.class,
    DynamicIndexClientBasicSelfTest.class,
    IgniteCacheSqlQueryErrorSelfTest.class,
    IgniteCacheSqlDmlErrorSelfTest.class,
    SqlUnsupportedSelfTest.class,
    IgniteCacheDuplicateEntityConfigurationSelfTest.class,
    IncorrectQueryEntityTest.class,
    IgniteDynamicSqlRestoreTest.class,
    IgniteSqlSplitterSelfTest.class,
    IgniteSqlSegmentedIndexSelfTest.class,
    GridCacheQueryIndexDisabledSelfTest.class,
    IgniteCacheReplicatedQueryP2PDisabledSelfTest.class,
    IgniteCachePartitionedQuerySelfTest.class,
    IgniteCachePartitionedSnapshotEnabledQuerySelfTest.class,
    IgniteCacheAtomicNearEnabledQuerySelfTest.class,
    IgniteCachePartitionedQueryP2PDisabledSelfTest.class,
    IgniteCacheParallelismQuerySortOrderTest.class,
    IgniteCacheJoinPartitionedAndReplicatedCollocationTest.class,
    IgniteClientReconnectCacheQueriesFailoverTest.class,
    CacheOffheapBatchIndexingMultiTypeTest.class,
    GridCacheQueryInternalKeysSelfTest.class,
    IgniteSqlBigIntegerKeyTest.class,
    IgniteCacheOffheapEvictQueryTest.class,
    IgniteBinaryObjectFieldsQuerySelfTest.class,
    IgniteCacheQueryNoRebalanceSelfTest.class,
    GridCacheQueryTransformerSelfTest.class,
    CacheScanQueryFailoverTest.class,
    IgniteCachePrimitiveFieldsQuerySelfTest.class,
    IgniteCacheJoinQueryWithAffinityKeyTest.class,
    IgniteCacheDeleteSqlQuerySelfTest.class,
    IgniteSqlSkipReducerOnUpdateDmlSelfTest.class,
    IgniteSqlSkipReducerOnUpdateDmlFlagSelfTest.class,
    IgniteCacheSqlInsertValidationSelfTest.class,
    H2DynamicIndexTransactionalPartitionedNearSelfTest.class,
    H2DynamicIndexAtomicPartitionedSelfTest.class,
    H2DynamicTableSelfTest.class,
    H2DynamicColumnsServerBasicSelfTest.class,
    H2DynamicIndexingComplexClientAtomicPartitionedTest.class,
    H2DynamicIndexingComplexClientAtomicPartitionedNoBackupsTest.class,
    H2DynamicIndexingComplexClientTransactionalPartitionedNoBackupsTest.class,
    H2DynamicIndexingComplexServerAtomicPartitionedTest.class,
    H2DynamicIndexingComplexServerAtomicPartitionedNoBackupsTest.class,
    H2DynamicIndexingComplexServerAtomicReplicatedTest.class,
    H2DynamicIndexingComplexServerTransactionalPartitionedTest.class,
    H2DynamicIndexingComplexServerTransactionalReplicatedTest.class,
})
public class IgniteBinaryCacheQueryTestSuite6 {
}
