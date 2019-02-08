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

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedTxPessimisticOriginatingNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePartitionedNearDisabledPrimaryNodeFailureRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePartitionedTwoBackupsPrimaryNodeFailureRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxPessimisticOriginatingNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.mvcc.ForwardCursorFailureReproducer;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

/** */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//    // Simple tests.
//    MvccEmptyTransactionSelfTest.class,
//    CacheMvccSqlConfigurationValidationTest.class,
//    CacheMvccDmlSimpleTest.class,
//    SqlTransactionsCommandsWithMvccEnabledSelfTest.class,
//    CacheMvccSizeTest.class,
//    CacheMvccSqlUpdateCountersTest.class,
//    CacheMvccSqlLockTimeoutTest.class,
//    CacheMvccSqlTxModesTest.class,
//    GridIndexRebuildWithMvccEnabledSelfTest.class,
//
//    CacheMvccTxNodeMappingTest.class,
//
//    MvccDeadlockDetectionConfigTest.class,
//    MvccDeadlockDetectionTest.class,
//
//    // SQL vs CacheAPI consistency.
//    MvccRepeatableReadOperationsTest.class,
//    MvccRepeatableReadBulkOpsTest.class,
//
//    // JDBC tests.
//    CacheMvccSizeWithConcurrentJdbcTransactionTest.class,
//    CacheMvccScanQueryWithConcurrentJdbcTransactionTest.class,
//    CacheMvccLocalEntriesWithConcurrentJdbcTransactionTest.class,
//    CacheMvccIteratorWithConcurrentJdbcTransactionTest.class,
//
//    // Load tests.
//    CacheMvccBulkLoadTest.class,
//    CacheMvccStreamingInsertTest.class,
//
//    CacheMvccPartitionedSqlQueriesTest.class,
//    CacheMvccReplicatedSqlQueriesTest.class,
//    CacheMvccPartitionedSqlTxQueriesTest.class,
//    CacheMvccReplicatedSqlTxQueriesTest.class,
//
//    CacheMvccPartitionedSqlTxQueriesWithReducerTest.class,
//    CacheMvccReplicatedSqlTxQueriesWithReducerTest.class,
//    CacheMvccPartitionedSelectForUpdateQueryTest.class,
//    CacheMvccReplicatedSelectForUpdateQueryTest.class,
//
//    // Failover tests.
//    CacheMvccPartitionedBackupsTest.class,
//    CacheMvccReplicatedBackupsTest.class,
//
//    CacheMvccPartitionedSqlCoordinatorFailoverTest.class,
//    CacheMvccReplicatedSqlCoordinatorFailoverTest.class,
//
//    // Continuous queries.
//    CacheMvccBasicContinuousQueryTest.class,
//    CacheMvccContinuousQueryPartitionedSelfTest.class,
//    CacheMvccContinuousQueryReplicatedSelfTest.class,
//    CacheMvccSqlContinuousQueryPartitionedSelfTest.class,
//    CacheMvccSqlContinuousQueryReplicatedSelfTest.class,
//
//    CacheMvccContinuousQueryPartitionedTxOneNodeTest.class,
//    CacheMvccContinuousQueryReplicatedTxOneNodeTest.class,
//
//    CacheMvccContinuousQueryClientReconnectTest.class,
//    CacheMvccContinuousQueryClientTest.class,
//
//    CacheMvccContinuousQueryMultiNodesFilteringTest.class,
//    CacheMvccContinuousQueryBackupQueueTest.class,
//    CacheMvccContinuousQueryImmutableEntryTest.class,
//    CacheMvccClientReconnectContinuousQueryTest.class,
//
//    CacheMvccContinuousWithTransformerClientSelfTest.class,
//    CacheMvccContinuousWithTransformerPartitionedSelfTest.class,
//    CacheMvccContinuousWithTransformerReplicatedSelfTest.class,
//
//    // Transaction recovery.
//    CacheMvccTxRecoveryTest.class,
//
//    IgniteCacheMvccSqlTestSuite.MvccPartitionedPrimaryNodeFailureRecoveryTest.class,
//    IgniteCacheMvccSqlTestSuite.MvccPartitionedTwoBackupsPrimaryNodeFailureRecoveryTest.class,
//    IgniteCacheMvccSqlTestSuite.MvccColocatedTxPessimisticOriginatingNodeFailureRecoveryTest.class,
//    IgniteCacheMvccSqlTestSuite.MvccReplicatedTxPessimisticOriginatingNodeFailureRecoveryTest.class
    ForwardCursorFailureReproducer.class
})
public class IgniteCacheMvccSqlTestSuite {
    /** */
    public static class MvccPartitionedPrimaryNodeFailureRecoveryTest
        extends IgniteCachePartitionedNearDisabledPrimaryNodeFailureRecoveryTest {
        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return TRANSACTIONAL_SNAPSHOT;
        }
    }

    /** */
    public static class MvccPartitionedTwoBackupsPrimaryNodeFailureRecoveryTest
        extends IgniteCachePartitionedTwoBackupsPrimaryNodeFailureRecoveryTest {
        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return TRANSACTIONAL_SNAPSHOT;
        }

        /** {@inheritDoc} */
        @Override protected NearCacheConfiguration nearConfiguration() {
            return null;
        }
    }

    /** */
    public static class MvccColocatedTxPessimisticOriginatingNodeFailureRecoveryTest
        extends GridCacheColocatedTxPessimisticOriginatingNodeFailureSelfTest {
        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return TRANSACTIONAL_SNAPSHOT;
        }
    }

    /** */
    public static class MvccReplicatedTxPessimisticOriginatingNodeFailureRecoveryTest
        extends GridCacheReplicatedTxPessimisticOriginatingNodeFailureSelfTest {
        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return TRANSACTIONAL_SNAPSHOT;
        }
    }
}
