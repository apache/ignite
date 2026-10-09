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

import org.apache.ignite.internal.processors.cache.AffinityKeyNameAndValueFieldNameConflictTest;
import org.apache.ignite.internal.processors.cache.CacheQueryBuildValueTest;
import org.apache.ignite.internal.processors.cache.DdlTransactionIndexingSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheCrossCacheQuerySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQuerySerializationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectLocalQueryArgumentsTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryObjectQueryArgumentsTest;
import org.apache.ignite.internal.processors.cache.IgniteBinaryWrappedObjectFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheCollocatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheInsertSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheJoinPartitionedAndReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLargeResultSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheMergeSqlQueryFailingTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheMergeSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheMultipleIndexedTypesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapIndexScanTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryH2IndexingLeakTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryIndexSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryLoadSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheUnionDuplicatesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheUpdateSqlQuerySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCrossCachesJoinsQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicEnableIndexingRestoreTest;
import org.apache.ignite.internal.processors.cache.IgniteErrorOnRebalanceTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCachePartitionedQueryEvtsDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQueryEvtsDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.index.ArrayIndexTest;
import org.apache.ignite.internal.processors.cache.index.BasicIndexTest;
import org.apache.ignite.internal.processors.cache.index.ComplexSecondaryKeyUnwrapSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexServerNodeFIlterBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicColumnsClientBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicColumnsServerCoordinatorBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexAtomicPartitionedNearSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexAtomicReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexTransactionalPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexTransactionalReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientAtomicReplicatedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientTransactionalPartitionedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexClientTransactionalReplicatedTest;
import org.apache.ignite.internal.processors.cache.index.H2DynamicIndexingComplexServerTransactionalPartitionedNoBackupsTest;
import org.apache.ignite.internal.processors.cache.query.CacheDataPageScanQueryTest;
import org.apache.ignite.internal.processors.cache.query.GridCircularQueueTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQueryTxSelfTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteStableBaselineBinObjFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.query.IgniteCachelessQueriesSelfTest;
import org.apache.ignite.internal.processors.query.IgniteQueryTableLockAndConnectionPoolSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSchemaIndexingTest;
import org.apache.ignite.internal.processors.query.IgniteSqlSegmentedIndexMultiNodeSelfTest;
import org.apache.ignite.internal.processors.query.MultipleStatementsSqlQuerySelfTest;
import org.apache.ignite.internal.processors.query.SqlIllegalSchemaSelfTest;
import org.apache.ignite.internal.processors.query.SqlNestedQuerySelfTest;
import org.apache.ignite.internal.processors.query.SqlPushDownFunctionTest;
import org.apache.ignite.internal.processors.query.h2.GridSubqueryJoinOptimizerSelfTest;
import org.apache.ignite.internal.processors.query.h2.H2ResultSetIteratorNullifyOnEndSelfTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlQueryMinMaxTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlQueryStartFinishListenerTest;
import org.apache.ignite.internal.processors.query.h2.QueryDataPageScanTest;
import org.apache.ignite.internal.processors.query.h2.sql.ExplainSelfTest;
import org.apache.ignite.internal.processors.query.h2.sql.GridQueryParsingTest;
import org.apache.ignite.internal.processors.sql.SqlConnectorConfigurationValidationSelfTest;
import org.apache.ignite.internal.sql.SqlParserBulkLoadSelfTest;
import org.apache.ignite.internal.sql.SqlParserDropIndexSelfTest;
import org.apache.ignite.internal.sql.SqlParserMultiStatementSelfTest;
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
    AffinityKeyNameAndValueFieldNameConflictTest.class,
    ComplexSecondaryKeyUnwrapSelfTest.class,
    SqlNestedQuerySelfTest.class,
    ExplainSelfTest.class,

    PartitionedSqlTest.class,
    ReplicatedSqlTest.class,
    ReplicatedSqlCustomPartitionsTest.class,
    CheckWarnJoinPartitionedTables.class,

    SqlParserDropIndexSelfTest.class,
    SqlParserBulkLoadSelfTest.class,
    SqlParserMultiStatementSelfTest.class,

    SqlConnectorConfigurationValidationSelfTest.class,

    SqlIllegalSchemaSelfTest.class,
    MultipleStatementsSqlQuerySelfTest.class,

    BasicIndexTest.class,
    ArrayIndexTest.class,

    // Misc tests.

    // Dynamic index create/drop tests.

    DynamicIndexServerBasicSelfTest.class,
    DynamicIndexServerNodeFIlterBasicSelfTest.class,

    // Parsing
    GridQueryParsingTest.class,

    // Config.
    IgniteDynamicEnableIndexingRestoreTest.class,

    // Queries tests.
    IgniteQueryTableLockAndConnectionPoolSelfTest.class,
    SqlPushDownFunctionTest.class,
    IgniteCachelessQueriesSelfTest.class,
    IgniteSqlSegmentedIndexMultiNodeSelfTest.class,
    IgniteSqlSchemaIndexingTest.class,
    IgniteCacheQueryLoadSelfTest.class,
    IgniteCacheReplicatedQuerySelfTest.class,
    IgniteCacheReplicatedQueryEvtsDisabledSelfTest.class,
    IgniteCacheAtomicQuerySelfTest.class,
    IgniteCachePartitionedQueryEvtsDisabledSelfTest.class,

    IgniteCacheUnionDuplicatesTest.class,
    IgniteErrorOnRebalanceTest.class,
    CacheQueryBuildValueTest.class,

    IgniteCacheQueryIndexSelfTest.class,
    IgniteCacheCollocatedQuerySelfTest.class,
    IgniteCacheLargeResultSelfTest.class,
    H2ResultSetIteratorNullifyOnEndSelfTest.class,
    IgniteCacheOffheapIndexScanTest.class,

    GridCacheCrossCacheQuerySelfTest.class,
    GridCacheQuerySerializationSelfTest.class,
    IgniteStableBaselineBinObjFieldsQuerySelfTest.class,
    IgniteBinaryWrappedObjectFieldsQuerySelfTest.class,
    IgniteCacheQueryH2IndexingLeakTest.class,

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

    IgniteBinaryObjectQueryArgumentsTest.class,
    IgniteBinaryObjectLocalQueryArgumentsTest.class,

    IndexingSpiQueryTxSelfTest.class,

    IgniteCacheMultipleIndexedTypesTest.class,
    IgniteSqlQueryMinMaxTest.class,
    IgniteSqlQueryStartFinishListenerTest.class,

    GridCircularQueueTest.class,

    // DDL.
    H2DynamicIndexTransactionalReplicatedSelfTest.class,
    H2DynamicIndexTransactionalPartitionedSelfTest.class,
    H2DynamicIndexAtomicReplicatedSelfTest.class,
    H2DynamicIndexAtomicPartitionedNearSelfTest.class,
    H2DynamicColumnsClientBasicSelfTest.class,
    H2DynamicColumnsServerCoordinatorBasicSelfTest.class,

    // DML+DDL.
    H2DynamicIndexingComplexClientAtomicReplicatedTest.class,
    H2DynamicIndexingComplexClientTransactionalPartitionedTest.class,
    H2DynamicIndexingComplexClientTransactionalReplicatedTest.class,
    H2DynamicIndexingComplexServerTransactionalPartitionedNoBackupsTest.class,

    DdlTransactionIndexingSelfTest.class,

})
public class IgniteBinaryCacheQueryTestSuite {
}
