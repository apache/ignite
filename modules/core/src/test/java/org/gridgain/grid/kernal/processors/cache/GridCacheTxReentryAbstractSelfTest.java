/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Tests reentry in pessimistic repeatable read tx.
 */
public abstract class GridCacheTxReentryAbstractSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** @return Cache mode. */
    protected abstract GridCacheMode cacheMode();

    /** @return Near enabled. */
    protected abstract boolean nearEnabled();

    /** @return Grid count. */
    protected abstract int gridCount();

    /** @return Test key. */
    protected abstract int testKey();

    /** @return Expected number of near lock requests. */
    protected abstract int expectedNearLockRequests();

    /** @return Expected number of near lock requests. */
    protected abstract int expectedDhtLockRequests();

    /** @return Expected number of near lock requests. */
    protected abstract int expectedDistributedLockRequests();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new CountingCommunicationSpi());
        cfg.setDiscoverySpi(discoSpi);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setDistributionMode(nearEnabled() ? NEAR_PARTITIONED : PARTITIONED_ONLY);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testLockReentry() throws Exception {
        startGrids(gridCount());

        try {
            GridCache<Object, Object> cache = grid(0).cache(null);

            // Find test key.
            int key = testKey();

            try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                // One near lock request.
                cache.get(key);

                // No more requests.
                cache.remove(key);

                tx.commit();
            }

            CountingCommunicationSpi commSpi = (CountingCommunicationSpi)grid(0).configuration().getCommunicationSpi();

            assertEquals(expectedNearLockRequests(), commSpi.nearLocks());
            assertEquals(expectedDhtLockRequests(), commSpi.dhtLocks());
            assertEquals(expectedDistributedLockRequests(), commSpi.distributedLocks());
        }
        finally {
            stopAllGrids();
        }
    }

    /** Counting communication SPI. */
    protected static class CountingCommunicationSpi extends TcpCommunicationSpi {
        /** Distributed lock requests. */
        private AtomicInteger distLocks = new AtomicInteger();

        /** Near lock requests. */
        private AtomicInteger nearLocks = new AtomicInteger();

        /** Dht locks. */
        private AtomicInteger dhtLocks = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, GridTcpCommunicationMessageAdapter msg)
            throws IgniteSpiException {
            countMsg((GridIoMessage)msg);

            super.sendMessage(node, msg);
        }

        /**
         * Unmarshals the message and increments counters.
         *
         * @param msg Message to check.
         */
        private void countMsg(GridIoMessage msg) {
            Object origMsg = msg.message();

            if (origMsg instanceof GridDistributedLockRequest) {
                distLocks.incrementAndGet();

                if (origMsg instanceof GridNearLockRequest)
                    nearLocks.incrementAndGet();
                else if (origMsg instanceof GridDhtLockRequest)
                    dhtLocks.incrementAndGet();
            }
        }

        /** @return Number of recorded distributed locks. */
        public int distributedLocks() {
            return distLocks.get();
        }

        /** @return Number of recorded distributed locks. */
        public int nearLocks() {
            return nearLocks.get();
        }

        /** @return Number of recorded distributed locks. */
        public int dhtLocks() {
            return dhtLocks.get();
        }
    }
}
