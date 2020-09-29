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

import java.security.Security;
import org.apache.ignite.internal.jdbc2.JdbcBlobTest;
import org.apache.ignite.internal.jdbc2.JdbcBulkLoadSelfTest;
import org.apache.ignite.internal.jdbc2.JdbcConnectionReopenTest;
import org.apache.ignite.internal.jdbc2.JdbcDistributedJoinsQueryTest;
import org.apache.ignite.internal.jdbc2.JdbcSchemaCaseSelfTest;
import org.apache.ignite.jdbc.JdbcAuthorizationTest;
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
import org.apache.ignite.jdbc.JdbcThinMetadataSqlMatchTest;
import org.apache.ignite.jdbc.thin.JdbcThinAuthenticateConnectionSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinAutoCloseServerCursorTest;
import org.apache.ignite.jdbc.thin.JdbcThinBatchSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinComplexDmlDdlCustomSchemaSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinComplexDmlDdlSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinComplexDmlDdlSkipReducerOnUpdateSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinComplexQuerySelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionAdditionalSecurityTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionMultipleAddressesTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionMvccEnabledSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionPropertiesTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionSSLTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionTimeoutSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDataPageScanPropertySelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDataSourceSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDefaultTimeoutTest;
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
import org.apache.ignite.jdbc.thin.JdbcThinMultiStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinNoDefaultSchemaTest;
import org.apache.ignite.jdbc.thin.JdbcThinPreparedStatementLeakTest;
import org.apache.ignite.jdbc.thin.JdbcThinPreparedStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinResultSetSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinSchemaCaseSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinSelectAfterAlterTable;
import org.apache.ignite.jdbc.thin.JdbcThinSqlMergeTest;
import org.apache.ignite.jdbc.thin.JdbcThinStatementCancelSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinStatementTimeoutSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinStreamingNotOrderedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinStreamingOrderedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinStreamingResetStreamTest;
import org.apache.ignite.jdbc.thin.JdbcThinTcpIoTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsClientAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsClientNoAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsLeaksMvccTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsServerAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsServerNoAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinUpdateStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinUpdateStatementSkipReducerOnUpdateSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinWalModeChangeSelfTest;
import org.apache.ignite.qa.QaJdbcTestSuite;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * JDBC driver test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    QaJdbcTestSuite.class,

    JdbcConnectionSelfTest.class,
    JdbcStatementSelfTest.class,
    JdbcPreparedStatementSelfTest.class,
    JdbcResultSetSelfTest.class,
    JdbcComplexQuerySelfTest.class,
    JdbcMetadataSelfTest.class,
    JdbcEmptyCacheSelfTest.class,
    JdbcLocalCachesSelfTest.class,
    JdbcNoDefaultCacheTest.class,
    JdbcDefaultNoOpCacheTest.class,
    JdbcPojoQuerySelfTest.class,
    JdbcPojoLegacyQuerySelfTest.class,
    JdbcConnectionReopenTest.class,
    JdbcAuthorizationTest.class,

    // Ignite client node based driver tests
    org.apache.ignite.internal.jdbc2.JdbcConnectionSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcSpringSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcStatementSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcPreparedStatementSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcResultSetSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcComplexQuerySelfTest.class,
    JdbcDistributedJoinsQueryTest.class,
    org.apache.ignite.internal.jdbc2.JdbcMetadataSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcEmptyCacheSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcLocalCachesSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcConnectionWithoutCacheNameTest.class,
    org.apache.ignite.internal.jdbc2.JdbcMergeStatementSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcBinaryMarshallerMergeStatementSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcUpdateStatementSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcInsertStatementSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcBinaryMarshallerInsertStatementSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcDeleteStatementSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcStatementBatchingSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcErrorsSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcStreamingToPublicCacheTest.class,
    org.apache.ignite.internal.jdbc2.JdbcNoCacheStreamingSelfTest.class,
    JdbcBulkLoadSelfTest.class,
    JdbcSchemaCaseSelfTest.class,

    JdbcBlobTest.class,
    org.apache.ignite.internal.jdbc2.JdbcStreamingSelfTest.class,
    JdbcThinStreamingNotOrderedSelfTest.class,
    JdbcThinStreamingOrderedSelfTest.class,
    JdbcThinDataPageScanPropertySelfTest.class,
    JdbcThinStreamingResetStreamTest.class,

    // DDL tests.
    org.apache.ignite.internal.jdbc2.JdbcDynamicIndexAtomicPartitionedNearSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcDynamicIndexAtomicPartitionedSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcDynamicIndexAtomicReplicatedSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcDynamicIndexTransactionalPartitionedNearSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcDynamicIndexTransactionalPartitionedSelfTest.class,
    org.apache.ignite.internal.jdbc2.JdbcDynamicIndexTransactionalReplicatedSelfTest.class,

    // New thin JDBC
    JdbcThinConnectionSelfTest.class,
    JdbcThinConnectionMvccEnabledSelfTest.class,
    JdbcThinConnectionMultipleAddressesTest.class,
    JdbcThinTcpIoTest.class,
    JdbcThinConnectionAdditionalSecurityTest.class,
    JdbcThinConnectionSSLTest.class,
    JdbcThinDataSourceSelfTest.class,
    JdbcThinPreparedStatementSelfTest.class,
    JdbcThinResultSetSelfTest.class,
    JdbcThinConnectionPropertiesTest.class,

    JdbcThinStatementSelfTest.class,
    JdbcThinComplexQuerySelfTest.class,
    JdbcThinNoDefaultSchemaTest.class,
    JdbcThinSchemaCaseSelfTest.class,
    JdbcThinEmptyCacheSelfTest.class,
    JdbcThinMetadataSelfTest.class,
    JdbcThinMetadataPrimaryKeysSelfTest.class,
    JdbcThinMetadataSqlMatchTest.class,
    JdbcThinErrorsSelfTest.class,
    JdbcThinStatementCancelSelfTest.class,
    JdbcThinStatementTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,
    JdbcThinDefaultTimeoutTest.class,

    JdbcThinInsertStatementSelfTest.class,
    JdbcThinUpdateStatementSelfTest.class,
    JdbcThinMergeStatementSelfTest.class,
    JdbcThinDeleteStatementSelfTest.class,
    JdbcThinAutoCloseServerCursorTest.class,
    JdbcThinBatchSelfTest.class,
    JdbcThinMissingLongArrayResultsTest.class,

    // New thin JDBC driver, DDL tests
    JdbcThinDynamicIndexAtomicPartitionedNearSelfTest.class,
    JdbcThinDynamicIndexAtomicPartitionedSelfTest.class,
    JdbcThinDynamicIndexAtomicReplicatedSelfTest.class,
    JdbcThinDynamicIndexTransactionalPartitionedNearSelfTest.class,
    JdbcThinDynamicIndexTransactionalPartitionedSelfTest.class,
    JdbcThinDynamicIndexTransactionalReplicatedSelfTest.class,
    JdbcThinMultiStatementSelfTest.class,

    // New thin JDBC driver, DML tests
    JdbcThinBulkLoadSelfTest.class,

    // New thin JDBC driver, full SQL tests
    JdbcThinComplexDmlDdlSelfTest.class,

    JdbcThinSelectAfterAlterTable.class,

    // Update on server
    JdbcThinInsertStatementSkipReducerOnUpdateSelfTest.class,
    JdbcThinUpdateStatementSkipReducerOnUpdateSelfTest.class,
    JdbcThinMergeStatementSkipReducerOnUpdateSelfTest.class,
    JdbcThinComplexDmlDdlSkipReducerOnUpdateSelfTest.class,
    JdbcThinComplexDmlDdlCustomSchemaSelfTest.class,

    // Transactions
    JdbcThinTransactionsSelfTest.class,
    JdbcThinTransactionsClientAutoCommitComplexSelfTest.class,
    JdbcThinTransactionsServerAutoCommitComplexSelfTest.class,
    JdbcThinTransactionsClientNoAutoCommitComplexSelfTest.class,
    JdbcThinTransactionsServerNoAutoCommitComplexSelfTest.class,

    JdbcThinLocalQueriesSelfTest.class,

    // Various commands.
    JdbcThinWalModeChangeSelfTest.class,
    JdbcThinAuthenticateConnectionSelfTest.class,

    JdbcThinPreparedStatementLeakTest.class,
    JdbcThinTransactionsLeaksMvccTest.class,
    JdbcThinSqlMergeTest.class,
})
public class IgniteJdbcDriverTestSuite {
    /**
     * Enable NULL algorithm and keep 3DES_EDE_CBC disabled.
     * See {@link JdbcThinConnectionSSLTest#testDisabledCustomCipher()} for details.
     */
    @BeforeClass
    public static void init() {
        Security.setProperty("jdk.tls.disabledAlgorithms", "3DES_EDE_CBC");
    }
}
