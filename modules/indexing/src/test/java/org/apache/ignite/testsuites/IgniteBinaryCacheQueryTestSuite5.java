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

import org.apache.ignite.internal.processors.cache.CacheScanPartitionQueryFallbackSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryEvictsMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheQueryMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.cache.QueryJoinWithDifferentNodeFiltersTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheClientQueryReplicatedNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeFailTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteSqlQueryWithBaselineTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentAtomicReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicColumnsConcurrentTransactionalPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicEnableIndexingBasicSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicEnableIndexingConcurrentSelfTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexCreateAfterClusterRestartTest;
import org.apache.ignite.internal.processors.cache.index.DynamicIndexReplicatedAtomicConcurrentSelfTest;
import org.apache.ignite.internal.processors.query.CreateIndexOnInvalidDataTypeTest;
import org.apache.ignite.internal.processors.query.DisabledSqlFunctionsTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsCompareQueryTest;
import org.apache.ignite.internal.processors.query.IgniteCacheGroupsSqlSegmentedIndexSelfTest;
import org.apache.ignite.internal.processors.query.LazyOnDmlTest;
import org.apache.ignite.internal.processors.query.SqlIndexConsistencyAfterInterruptTxCacheOperationTest;
import org.apache.ignite.internal.processors.query.SqlTwoCachesInGroupWithSameEntryTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries. Split off from {@link IgniteBinaryCacheQueryTestSuite2} to reduce
 * the single-suite runtime in CI; contains an independent subset of the same test classes.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    LazyOnDmlTest.class,
    CreateIndexOnInvalidDataTypeTest.class,
    DisabledSqlFunctionsTest.class,
    SqlIndexConsistencyAfterInterruptTxCacheOperationTest.class,
    SqlTwoCachesInGroupWithSameEntryTest.class,
    DynamicIndexReplicatedAtomicConcurrentSelfTest.class,
    DynamicIndexCreateAfterClusterRestartTest.class,
    DynamicColumnsConcurrentTransactionalPartitionedSelfTest.class,
    DynamicColumnsConcurrentAtomicReplicatedSelfTest.class,
    DynamicEnableIndexingBasicSelfTest.class,
    DynamicEnableIndexingConcurrentSelfTest.class,
    IgniteCacheQueryMultiThreadedSelfTest.class,
    IgniteCacheQueryEvictsMultiThreadedSelfTest.class,
    IgniteCacheClientQueryReplicatedNodeRestartSelfTest.class,
    IgniteCacheQueryNodeFailTest.class,
    IgniteCacheQueryNodeRestartSelfTest.class,
    IgniteSqlQueryWithBaselineTest.class,
    CacheScanPartitionQueryFallbackSelfTest.class,
    IgniteCacheGroupsCompareQueryTest.class,
    IgniteCacheGroupsSqlSegmentedIndexSelfTest.class,
    QueryJoinWithDifferentNodeFiltersTest.class,
})
public class IgniteBinaryCacheQueryTestSuite5 {
}
