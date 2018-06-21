package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsBinaryMetadataOnClusterRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCorruptedIndexTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsCacheDestroyDuringCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsDiskErrorsRecoveringTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsNoActualWalHistoryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.*;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeWithIndexingWalRestoreTest;

/**
 * PDS test for WAL and recovery.
 */
public class IgnitePdsWithIndexingWalAndRecoveryTestSuite extends TestSuite {

    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Persistent Store With Indexing WAL&Recovery Test Suite");

        suite.addTestSuite(WalPathsTest.class);

        suite.addTestSuite(IgniteDbSingleNodeWithIndexingWalRestoreTest.class);

        suite.addTestSuite(WalRecoveryTxLogicalRecordsTest.class);
        suite.addTestSuite(IgniteWalRecoveryTest.class);
        suite.addTestSuite(IgniteWalRecoveryPPCTest.class);
        suite.addTestSuite(IgniteWalRecoveryWithCompactionTest.class);

        suite.addTestSuite(IgniteWalRebalanceTest.class);

        suite.addTestSuite(IgnitePdsNoActualWalHistoryTest.class);

        suite.addTestSuite(IgnitePdsDiskErrorsRecoveringTest.class);

        suite.addTestSuite(IgnitePdsCacheDestroyDuringCheckpointTest.class);

        suite.addTestSuite(IgnitePdsBinaryMetadataOnClusterRestartTest.class);

        suite.addTestSuite(IgnitePdsCorruptedIndexTest.class);

        return suite;
    }

}
