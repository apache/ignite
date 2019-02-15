/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite.testsuites;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsAtomicCacheHistoricalRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsAtomicCacheRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsBinaryMetadataOnClusterRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsBinarySortObjectFieldsTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCorruptedIndexTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsMarshallerMappingRestoreOnNodeStartTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsTxCacheRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsTxHistoricalRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePersistentStoreCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.persistence.PersistenceDirectoryWarningLoggingTest;
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
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalPathsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalRecoveryTxLogicalRecordsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalRolloverRecordLoggingFsyncTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalRolloverRecordLoggingLogOnlyTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite for tests that cover core PDS features and depend on indexing module.
 */
@RunWith(AllTests.class)
public class IgnitePdsWithIndexingCoreTestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Persistent Store With Indexing Test Suite");

        suite.addTest(new JUnit4TestAdapter(IgnitePdsCacheIntegrationTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsPageEvictionTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsMultiNodePutGetRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePersistentStoreCacheGroupsTest.class));
        suite.addTest(new JUnit4TestAdapter(PersistenceDirectoryWarningLoggingTest.class));
        suite.addTest(new JUnit4TestAdapter(WalPathsTest.class));
        suite.addTest(new JUnit4TestAdapter(WalRecoveryTxLogicalRecordsTest.class));
        suite.addTest(new JUnit4TestAdapter(WalRolloverRecordLoggingFsyncTest.class));
        suite.addTest(new JUnit4TestAdapter(WalRolloverRecordLoggingLogOnlyTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalRecoveryTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteWalRecoveryWithCompactionTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsNoActualWalHistoryTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteWalRebalanceTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsAtomicCacheRebalancingTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsAtomicCacheHistoricalRebalancingTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsTxCacheRebalancingTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsTxHistoricalRebalancingTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteWalRecoveryPPCTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsDiskErrorsRecoveringTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsCacheDestroyDuringCheckpointTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsBinaryMetadataOnClusterRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsMarshallerMappingRestoreOnNodeStartTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsThreadInterruptionTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsBinarySortObjectFieldsTest.class));

        suite.addTest(new JUnit4TestAdapter(IgnitePdsCorruptedIndexTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteLogicalRecoveryTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteSequentialNodeCrashRecoveryTest.class));

        return suite;
    }
}
