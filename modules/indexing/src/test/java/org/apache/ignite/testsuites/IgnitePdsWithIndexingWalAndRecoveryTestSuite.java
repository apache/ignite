package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsBinaryMetadataOnClusterRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCorruptedIndexTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsCacheDestroyDuringCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsDiskErrorsRecoveringTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsNoActualWalHistoryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRecoveryPPCTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRecoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRecoveryWithCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalPathsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalRecoveryTxLogicalRecordsTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeWithIndexingWalRestoreTest;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * PDS test for WAL and recovery.
 */
public class IgnitePdsWithIndexingWalAndRecoveryTestSuite extends TestSuite {
    /**
     *
     */
    public static TestSuite suite() {
        System.setProperty(GridAbstractTest.PERSISTENCE_IN_TESTS_IS_ALLOWED_PROPERTY, "true");

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
