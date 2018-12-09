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

package org.apache.ignite.jdbc.suite;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.jdbc2.JdbcBlobTest;
import org.apache.ignite.internal.jdbc2.JdbcBulkLoadSelfTest;
import org.apache.ignite.internal.jdbc2.JdbcConnectionReopenTest;
import org.apache.ignite.internal.jdbc2.JdbcDistributedJoinsQueryTest;
import org.apache.ignite.jdbc.JdbcComplexQuerySelfTest;
import org.apache.ignite.jdbc.JdbcConnectionSelfTest;
import org.apache.ignite.jdbc.JdbcDefaultNoOpCacheTest;
import org.apache.ignite.jdbc.JdbcEmptyCacheSelfTest;
import org.apache.ignite.jdbc.JdbcLocalCachesSelfTest;
import org.apache.ignite.jdbc.JdbcMetadataSelfTest;
import org.apache.ignite.jdbc.JdbcNoDefaultCacheTest;
import org.apache.ignite.jdbc.JdbcPojoLegacyQuerySelfTest;
import org.apache.ignite.jdbc.JdbcPojoQuerySelfTest;
import org.apache.ignite.jdbc.JdbcPreparedStatementSelfTest;
import org.apache.ignite.jdbc.JdbcResultSetSelfTest;
import org.apache.ignite.jdbc.JdbcStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinAuthenticateConnectionSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinAutoCloseServerCursorTest;
import org.apache.ignite.jdbc.thin.JdbcThinBatchSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadAtomicPartitionedNearSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadAtomicPartitionedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadAtomicReplicatedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadTransactionalPartitionedNearSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadTransactionalPartitionedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadTransactionalReplicatedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinComplexDmlDdlCustomSchemaSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinComplexDmlDdlSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinComplexDmlDdlSkipReducerOnUpdateSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinComplexQuerySelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionMultipleAddressesTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionMvccEnabledSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionSSLTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDataSourceSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDeleteStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDynamicIndexAtomicPartitionedNearSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDynamicIndexAtomicPartitionedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDynamicIndexAtomicReplicatedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDynamicIndexTransactionalPartitionedNearSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDynamicIndexTransactionalPartitionedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDynamicIndexTransactionalReplicatedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinEmptyCacheSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinErrorsSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinInsertStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinInsertStatementSkipReducerOnUpdateSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinLocalQueriesSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinMergeStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinMergeStatementSkipReducerOnUpdateSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinMetadataPrimaryKeysSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinMetadataSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinMissingLongArrayResultsTest;
import org.apache.ignite.jdbc.thin.JdbcThinNoDefaultSchemaTest;
import org.apache.ignite.jdbc.thin.JdbcThinPreparedStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinResultSetSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinSchemaCaseTest;
import org.apache.ignite.jdbc.thin.JdbcThinSelectAfterAlterTable;
import org.apache.ignite.jdbc.thin.JdbcThinStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinStreamingNotOrderedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinStreamingOrderedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTcpIoTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsClientAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsClientNoAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsServerAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsServerNoAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinUpdateStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinUpdateStatementSkipReducerOnUpdateSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinWalModeChangeSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * JDBC driver test suite.
 */
@RunWith(AllTests.class)
public class IgniteJdbcDriverTestSuite {
    /**
     * @return JDBC Driver Test Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite JDBC Driver Test Suite");

        // Thin client based driver tests.
        suite.addTest(new JUnit4TestAdapter(JdbcConnectionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcPreparedStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcResultSetSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcComplexQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcMetadataSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcEmptyCacheSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcLocalCachesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcNoDefaultCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcDefaultNoOpCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcPojoQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcPojoLegacyQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcConnectionReopenTest.class));

        // Ignite client node based driver tests
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcConnectionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcSpringSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcPreparedStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcResultSetSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcComplexQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcDistributedJoinsQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcMetadataSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcEmptyCacheSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcLocalCachesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcNoDefaultCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcDefaultNoOpCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcMergeStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcBinaryMarshallerMergeStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcUpdateStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcInsertStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcBinaryMarshallerInsertStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcDeleteStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcStatementBatchingSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcErrorsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcStreamingToPublicCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcNoCacheStreamingSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcBulkLoadSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(JdbcBlobTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcStreamingSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinStreamingNotOrderedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinStreamingOrderedSelfTest.class));

        // DDL tests.
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcDynamicIndexAtomicPartitionedNearSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcDynamicIndexAtomicPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcDynamicIndexAtomicReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcDynamicIndexTransactionalPartitionedNearSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcDynamicIndexTransactionalPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(org.apache.ignite.internal.jdbc2.JdbcDynamicIndexTransactionalReplicatedSelfTest.class));

        // New thin JDBC
        suite.addTest(new JUnit4TestAdapter(JdbcThinConnectionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinConnectionMvccEnabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinConnectionMultipleAddressesTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinTcpIoTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinConnectionSSLTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinDataSourceSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinPreparedStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinResultSetSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(JdbcThinStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinComplexQuerySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinNoDefaultSchemaTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinSchemaCaseTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinEmptyCacheSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinMetadataSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinMetadataPrimaryKeysSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinErrorsSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(JdbcThinInsertStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinUpdateStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinMergeStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinDeleteStatementSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinAutoCloseServerCursorTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinBatchSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinMissingLongArrayResultsTest.class));

        // New thin JDBC driver, DDL tests
        suite.addTest(new JUnit4TestAdapter(JdbcThinDynamicIndexAtomicPartitionedNearSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinDynamicIndexAtomicPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinDynamicIndexAtomicReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinDynamicIndexTransactionalPartitionedNearSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinDynamicIndexTransactionalPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinDynamicIndexTransactionalReplicatedSelfTest.class));

        // New thin JDBC driver, DML tests
        suite.addTest(new JUnit4TestAdapter(JdbcThinBulkLoadAtomicPartitionedNearSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinBulkLoadAtomicPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinBulkLoadAtomicReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinBulkLoadTransactionalPartitionedNearSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinBulkLoadTransactionalPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinBulkLoadTransactionalReplicatedSelfTest.class));

        // New thin JDBC driver, full SQL tests
        suite.addTest(new JUnit4TestAdapter(JdbcThinComplexDmlDdlSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(JdbcThinSelectAfterAlterTable.class));

        // Update on server
        suite.addTest(new JUnit4TestAdapter(JdbcThinInsertStatementSkipReducerOnUpdateSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinUpdateStatementSkipReducerOnUpdateSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinMergeStatementSkipReducerOnUpdateSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinComplexDmlDdlSkipReducerOnUpdateSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinComplexDmlDdlCustomSchemaSelfTest.class));

        // Transactions
        suite.addTest(new JUnit4TestAdapter(JdbcThinTransactionsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinTransactionsClientAutoCommitComplexSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinTransactionsServerAutoCommitComplexSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinTransactionsClientNoAutoCommitComplexSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinTransactionsServerNoAutoCommitComplexSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(JdbcThinLocalQueriesSelfTest.class));

        // Various commands.
        suite.addTest(new JUnit4TestAdapter(JdbcThinWalModeChangeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinAuthenticateConnectionSelfTest.class));

        return suite;
    }
}
