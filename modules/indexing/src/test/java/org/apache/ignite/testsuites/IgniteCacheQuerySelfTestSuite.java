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
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.distributed.replicated.*;
import org.apache.ignite.internal.processors.cache.index.*;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalAtomicQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQueryCancelOrTimeoutSelfTest;
import org.apache.ignite.internal.processors.cache.local.IgniteCacheLocalQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryTransformerSelfTest;
import org.apache.ignite.internal.processors.cache.query.IgniteCacheQueryCacheDestroySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQueryTxSelfTest;
import org.apache.ignite.internal.processors.client.ClientConnectorConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.query.h2.GridH2IndexingInMemSelfTest;
import org.apache.ignite.internal.processors.query.h2.GridH2IndexingOffheapSelfTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlBigIntegerKeyTest;
import org.apache.ignite.internal.processors.query.h2.IgniteSqlQueryMinMaxTest;
import org.apache.ignite.internal.processors.query.h2.sql.BaseH2CompareQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.GridQueryParsingTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryDistributedJoinsTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryTest;
import org.apache.ignite.internal.processors.sql.SqlConnectorConfigurationValidationSelfTest;
import org.apache.ignite.internal.sql.SqlParserCreateIndexSelfTest;
import org.apache.ignite.internal.sql.SqlParserDropIndexSelfTest;
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

        suite.addTestSuite(SqlParserCreateIndexSelfTest.class);
        suite.addTestSuite(SqlParserDropIndexSelfTest.class);

        suite.addTestSuite(SqlConnectorConfigurationValidationSelfTest.class);
        suite.addTestSuite(ClientConnectorConfigurationValidationSelfTest.class);

        suite.addTestSuite(SqlSchemaSelfTest.class);
        suite.addTestSuite(MultipleStatementsSqlQuerySelfTest.class);

        // Misc tests.
        // TODO: Enable when IGNITE-1094 is fixed.
        // suite.addTest(new TestSuite(QueryEntityValidationSelfTest.class));
        suite.addTest(new TestSuite(DuplicateKeyValueClassesSelfTest.class));

        // Dynamic index create/drop tests.
        suite.addTest(new TestSuite(SchemaExchangeSelfTest.class));

        suite.addTest(new TestSuite(DynamicIndexServerCoordinatorBasicSelfTest.class));
        suite.addTest(new TestSuite(DynamicIndexServerBasicSelfTest.class));
        suite.addTest(new TestSuite(DynamicIndexServerNodeFilterCoordinatorBasicSelfTest.class));
        suite.addTest(new TestSuite(DynamicIndexServerNodeFIlterBasicSelfTest.class));
        suite.addTest(new TestSuite(DynamicIndexClientBasicSelfTest.class));

        // H2 tests.

        // TODO: IGNITE-4994: Restore mock.
        // suite.addTest(new TestSuite(GridH2TableSelfTest.class));

        suite.addTest(new TestSuite(GridH2IndexingInMemSelfTest.class));
        suite.addTest(new TestSuite(GridH2IndexingOffheapSelfTest.class));

        // Parsing
        suite.addTestSuite(GridQueryParsingTest.class);

        // Config.
        suite.addTestSuite(IgniteCacheDuplicateEntityConfigurationSelfTest.class);
        suite.addTestSuite(IncorrectQueryEntityTest.class);

        // Queries tests.
        suite.addTestSuite(LazyQuerySelfTest.class);
        suite.addTestSuite(IgniteSqlSplitterSelfTest.class);
        suite.addTestSuite(IgniteSqlSegmentedIndexSelfTest.class);
        suite.addTestSuite(IgniteSqlSegmentedIndexMultiNodeSelfTest.class);
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
        suite.addTestSuite(IgniteSqlBigIntegerKeyTest.class);

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
        suite.addTestSuite(IgniteCacheJoinPartitionedAndReplicatedTest.class);
        suite.addTestSuite(IgniteCrossCachesJoinsQueryTest.class);

        suite.addTestSuite(IgniteCacheMultipleIndexedTypesTest.class);

        // DML.
        suite.addTestSuite(IgniteCacheMergeSqlQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheInsertSqlQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheUpdateSqlQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheDeleteSqlQuerySelfTest.class);
        suite.addTestSuite(IgniteSqlSkipReducerOnUpdateDmlSelfTest.class);
        suite.addTestSuite(IgniteSqlSkipReducerOnUpdateDmlFlagSelfTest.class);

        suite.addTestSuite(IgniteBinaryObjectQueryArgumentsTest.class);
        suite.addTestSuite(IgniteBinaryObjectLocalQueryArgumentsTest.class);

        suite.addTestSuite(IndexingSpiQuerySelfTest.class);
        suite.addTestSuite(IndexingSpiQueryTxSelfTest.class);

        suite.addTestSuite(IgniteCacheMultipleIndexedTypesTest.class);
        suite.addTestSuite(IgniteSqlQueryMinMaxTest.class);

        // DDL.
        suite.addTestSuite(H2DynamicIndexTransactionalReplicatedSelfTest.class);
        suite.addTestSuite(H2DynamicIndexTransactionalPartitionedSelfTest.class);
        suite.addTestSuite(H2DynamicIndexTransactionalPartitionedNearSelfTest.class);
        suite.addTestSuite(H2DynamicIndexAtomicReplicatedSelfTest.class);
        suite.addTestSuite(H2DynamicIndexAtomicPartitionedSelfTest.class);
        suite.addTestSuite(H2DynamicIndexAtomicPartitionedNearSelfTest.class);
        suite.addTestSuite(H2DynamicTableSelfTest.class);
        suite.addTestSuite(H2DynamicColumnsClientBasicSelfTest.class);
        suite.addTestSuite(H2DynamicColumnsServerBasicSelfTest.class);
        suite.addTestSuite(H2DynamicColumnsServerCoordinatorBasicSelfTest.class);

        // DML+DDL.
        suite.addTestSuite(H2DynamicIndexingComplexClientAtomicPartitionedTest.class);
        suite.addTestSuite(H2DynamicIndexingComplexClientAtomicReplicatedTest.class);
        suite.addTestSuite(H2DynamicIndexingComplexClientTransactionalPartitionedTest.class);
        suite.addTestSuite(H2DynamicIndexingComplexClientTransactionalReplicatedTest.class);
        suite.addTestSuite(H2DynamicIndexingComplexServerAtomicPartitionedTest.class);
        suite.addTestSuite(H2DynamicIndexingComplexServerAtomicReplicatedTest.class);
        suite.addTestSuite(H2DynamicIndexingComplexServerTransactionalPartitionedTest.class);
        suite.addTestSuite(H2DynamicIndexingComplexServerTransactionalReplicatedTest.class);

        // Fields queries.
        suite.addTestSuite(SqlFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheLocalFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheReplicatedFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheReplicatedFieldsQueryROSelfTest.class);
        suite.addTestSuite(IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest.class);
        suite.addTestSuite(IgniteCachePartitionedFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheAtomicFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheAtomicNearEnabledFieldsQuerySelfTest.class);
        suite.addTestSuite(IgniteCachePartitionedFieldsQueryP2PEnabledSelfTest.class);
        suite.addTestSuite(IgniteCacheFieldsQueryNoDataSelfTest.class);
        suite.addTestSuite(GridCacheQueryIndexingDisabledSelfTest.class);
        suite.addTestSuite(GridOrderedMessageCancelSelfTest.class);
        suite.addTestSuite(CacheQueryEvictDataLostTest.class);

        // Full text queries.
        suite.addTestSuite(GridCacheFullTextQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheFullTextQueryNodeJoiningSelfTest.class);

        // Ignite cache and H2 comparison.
        suite.addTestSuite(BaseH2CompareQueryTest.class);
        suite.addTestSuite(H2CompareBigQueryTest.class);
        suite.addTestSuite(H2CompareBigQueryDistributedJoinsTest.class);

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

        // Distributed joins.
        suite.addTestSuite(H2CompareBigQueryDistributedJoinsTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinCollocatedAndNotTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinCustomAffinityMapper.class);
        suite.addTestSuite(IgniteCacheDistributedJoinNoIndexTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinPartitionedAndReplicatedTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinQueryConditionsTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinTest.class);
        suite.addTestSuite(IgniteSqlDistributedJoinSelfTest.class);

        // Other.
        suite.addTestSuite(CacheIteratorScanQueryTest.class);
        suite.addTestSuite(CacheQueryNewClientSelfTest.class);
        suite.addTestSuite(CacheOffheapBatchIndexingSingleTypeTest.class);
        suite.addTestSuite(CacheSqlQueryValueCopySelfTest.class);
        suite.addTestSuite(IgniteCacheQueryCacheDestroySelfTest.class);
        suite.addTestSuite(IgniteQueryDedicatedPoolTest.class);
        suite.addTestSuite(IgniteSqlEntryCacheModeAgnosticTest.class);
        suite.addTestSuite(QueryEntityCaseMismatchTest.class);
        suite.addTestSuite(IgniteCacheDistributedPartitionQuerySelfTest.class);
        suite.addTestSuite(IgniteCacheDistributedPartitionQueryNodeRestartsSelfTest.class);
        suite.addTestSuite(IgniteCacheDistributedPartitionQueryConfigurationSelfTest.class);
        suite.addTestSuite(IgniteSqlKeyValueFieldsTest.class);
        suite.addTestSuite(IgniteSqlRoutingTest.class);
        suite.addTestSuite(IgniteSqlNotNullConstraintTest.class);
        suite.addTestSuite(LongIndexNameTest.class);
        suite.addTestSuite(GridCacheQuerySqlFieldInlineSizeSelfTest.class);
        suite.addTestSuite(IgniteSqlParameterizedQueryTest.class);

        return suite;
    }
}
