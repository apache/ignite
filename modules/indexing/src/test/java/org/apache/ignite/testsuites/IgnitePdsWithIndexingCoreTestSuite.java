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

import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsAtomicCacheHistoricalRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsAtomicCacheRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsBinaryMetadataAsyncWritingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsBinaryMetadataOnClusterRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsBinarySortObjectFieldsTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCorruptedIndexTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsMarshallerMappingRestoreOnNodeStartTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsTxCacheRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsTxHistoricalRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePersistentStoreCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.persistence.PersistenceDirectoryWarningLoggingTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteCacheGroupsWithRestartsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteLogicalRecoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsMultiNodePutGetRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsPageEvictionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteSequentialNodeCrashRecoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsCacheDestroyDuringCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsCacheIntegrationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsDiskErrorsRecoveringTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsNoActualWalHistoryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsThreadInterruptionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRecoveryPPCTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRecoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRecoveryWithCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalOnNodeStartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalPathsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalRecoveryTxLogicalRecordsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalRolloverRecordLoggingFsyncTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalRolloverRecordLoggingLogOnlyTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for tests that cover core PDS features and depend on indexing module.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgnitePdsCacheIntegrationTest.class,
    IgnitePdsPageEvictionTest.class,
    IgnitePdsMultiNodePutGetRestartTest.class,
    IgnitePersistentStoreCacheGroupsTest.class,
    PersistenceDirectoryWarningLoggingTest.class,
    WalPathsTest.class,
    WalRecoveryTxLogicalRecordsTest.class,
    WalRolloverRecordLoggingFsyncTest.class,
    WalRolloverRecordLoggingLogOnlyTest.class,
    WalOnNodeStartTest.class,

    IgniteWalRecoveryTest.class,
    IgniteWalRecoveryWithCompactionTest.class,
    IgnitePdsNoActualWalHistoryTest.class,
    IgniteWalRebalanceTest.class,

    IgnitePdsAtomicCacheRebalancingTest.class,
    IgnitePdsAtomicCacheHistoricalRebalancingTest.class,

    IgnitePdsTxCacheRebalancingTest.class,
    IgnitePdsTxHistoricalRebalancingTest.class,

    IgniteWalRecoveryPPCTest.class,

    IgnitePdsDiskErrorsRecoveringTest.class,

    IgnitePdsCacheDestroyDuringCheckpointTest.class,

    IgnitePdsBinaryMetadataOnClusterRestartTest.class,
    IgnitePdsBinaryMetadataAsyncWritingTest.class,
    IgnitePdsMarshallerMappingRestoreOnNodeStartTest.class,
    IgnitePdsThreadInterruptionTest.class,
    IgnitePdsBinarySortObjectFieldsTest.class,

    IgnitePdsCorruptedIndexTest.class,

    IgniteLogicalRecoveryTest.class,

    IgniteSequentialNodeCrashRecoveryTest.class,

    IgniteCacheGroupsWithRestartsTest.class
})
public class IgnitePdsWithIndexingCoreTestSuite {
}
