/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
/**
 *
 */
public class WalRolloverRecordLoggingTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static class RolloverRecord extends CheckpointRecord {
        /** */
        private RolloverRecord() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override public boolean rollOver() {
            return true;
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(40 * 1024 * 1024))
            .setWalMode(LOG_ONLY)
            .setWalSegmentSize(4 * 1024 * 1024)
            .setWalArchivePath(DFLT_WAL_PATH));

        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler(false, 0));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    public void testAvoidInfinityWaitingOnRolloverOfSegment() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        long startTime = U.currentTimeMillis();
        long duration = 5_000;

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(
            () -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();

                while (U.currentTimeMillis() - startTime < duration)
                    cache.put(random.nextInt(100_000), random.nextInt(100_000));
            },
            8, "cache-put-thread");

        Thread t = new Thread(() -> {
            do {
                try {
                    U.sleep(100);
                }
                catch (IgniteInterruptedCheckedException e) {
                    // No-op.
                }

                ig.context().cache().context().database().wakeupForCheckpoint("test");
            } while (U.currentTimeMillis() - startTime < duration);
        });

        t.start();

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        IgniteCacheDatabaseSharedManager dbMgr = ig.context().cache().context().database();

        RolloverRecord rec = new RolloverRecord();

        do {
            try {
                dbMgr.checkpointReadLock();

                try {
                    walMgr.log(rec);
                }
                finally {
                    dbMgr.checkpointReadUnlock();
                }
            }
            catch (IgniteCheckedException e) {
                log.error(e.getMessage(), e);
            }
        } while (U.currentTimeMillis() - startTime < duration);

        fut.get();

        t.join();
    }
}
