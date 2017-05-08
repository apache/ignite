package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.cache.database.IgnitePersistentStoreAtomicCacheRebalancingTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreTxCacheRebalancingTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreContinuousRestartSelfTest;
import org.apache.ignite.cache.database.IgnitePersistentStorePageSizesTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreWalTlbSelfTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreRecoveryAfterFileCorruptionTest;
import org.apache.ignite.cache.database.db.DbPageEvictionDuringPartitionClearSelfTest;
import org.apache.ignite.cache.database.db.IgniteDbWholeClusterRestartSelfTest;
import org.apache.ignite.cache.database.db.RebalancingOnNotStableTopologyTest;
import org.apache.ignite.cache.database.db.TransactionsHangTest;
import org.apache.ignite.cache.database.db.file.IgniteNoActualWalHistorySelfTest;
import org.apache.ignite.cache.database.db.file.IgniteWalHistoryReservationsSelfTest;
import org.apache.ignite.cache.database.db.file.IgniteWalRecoverySelfTest;
import org.apache.ignite.cache.database.db.file.WalRecoveryTxLogicalRecordsTest;
import org.apache.ignite.cache.database.db.wal.crc.IgniteDataIntegrityTests;

/**
 *
 */
public class IgnitePersistentStoreTestSuit2 extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite persistent Store Test Suite");

        // Integrity test
        suite.addTestSuite(IgniteDataIntegrityTests.class);
        suite.addTestSuite(IgnitePersistentStoreRecoveryAfterFileCorruptionTest.class);
        suite.addTestSuite(IgnitePersistentStorePageSizesTest.class);
        suite.addTestSuite(IgnitePersistentStoreWalTlbSelfTest.class);

        // WAL recovery test.
        suite.addTestSuite(WalRecoveryTxLogicalRecordsTest.class);
        suite.addTestSuite(IgniteWalRecoverySelfTest.class);

        suite.addTestSuite(TransactionsHangTest.class);

        suite.addTestSuite(IgniteNoActualWalHistorySelfTest.class);

        suite.addTestSuite(RebalancingOnNotStableTopologyTest.class);

        suite.addTestSuite(IgniteDbWholeClusterRestartSelfTest.class);

        suite.addTestSuite(DbPageEvictionDuringPartitionClearSelfTest.class);

        // Rebalancing test
        suite.addTestSuite(IgnitePersistentStoreAtomicCacheRebalancingTest.class);
        suite.addTestSuite(IgnitePersistentStoreTxCacheRebalancingTest.class);
        suite.addTestSuite(IgniteWalHistoryReservationsSelfTest.class);

        suite.addTestSuite(IgnitePersistentStoreContinuousRestartSelfTest.class);

        return suite;
    }
}
