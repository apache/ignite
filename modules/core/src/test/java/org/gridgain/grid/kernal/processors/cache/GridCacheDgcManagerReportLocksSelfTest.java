/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;

/**
 * Cache DGC test. <p> Tests that locks are properly reported and not DGCed.
 */
public class GridCacheDgcManagerReportLocksSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 4;

    /** */
    private static final int KEYS_CNT = NODES_CNT * 2;

    /** */
    private static final int DGC_FREQ = 1000;

    /** */
    private static final int DGC_SUSPECT_LOCK_TIMEOUT = 1000;

    /** */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Grid counter. */
    private static AtomicInteger cntr = new AtomicInteger(0);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (Grid g : G.allGrids())
            for (GridCache<?, ?> cache : g.caches())
                cache.clearAll();
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration repCacheCfg = defaultCacheConfiguration();

        repCacheCfg.setName("replicated");

        repCacheCfg.setCacheMode(REPLICATED);
        repCacheCfg.setPreloadMode(SYNC);
        repCacheCfg.setDgcFrequency(DGC_FREQ);
        repCacheCfg.setDgcSuspectLockTimeout(DGC_SUSPECT_LOCK_TIMEOUT);
        repCacheCfg.setDgcRemoveLocks(false);
        repCacheCfg.setDefaultTxConcurrency(PESSIMISTIC);
        repCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_ASYNC);
        repCacheCfg.setEvictionPolicy(null);

        GridCacheConfiguration partCacheCfg = defaultCacheConfiguration();

        partCacheCfg.setName("partitioned");

        partCacheCfg.setCacheMode(PARTITIONED);
        partCacheCfg.setPreloadMode(SYNC);
        partCacheCfg.setDgcFrequency(DGC_FREQ);
        partCacheCfg.setDgcSuspectLockTimeout(DGC_SUSPECT_LOCK_TIMEOUT);
        partCacheCfg.setDgcRemoveLocks(false);
        partCacheCfg.setDefaultTxConcurrency(PESSIMISTIC);
        partCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_ASYNC);
        partCacheCfg.setEvictionPolicy(null);
        partCacheCfg.setNearEvictionPolicy(null);
        partCacheCfg.setAffinity(new GridCacheModuloAffinityFunction(NODES_CNT, 1));
        partCacheCfg.setAtomicityMode(TRANSACTIONAL);
        partCacheCfg.setDistributionMode(NEAR_PARTITIONED);

        cfg.setCacheConfiguration(repCacheCfg, partCacheCfg);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        // Override communication SPI.
        cfg.setCommunicationSpi(new TestCommunicationSpi());

        cfg.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR, cntr.getAndIncrement()));

        return cfg;
    }

    /**
     *
     */
    private void checkReplicatedLocks() {
        info("Checking locks.");

        for (int i = 0; i < KEYS_CNT; i++) {
            GridKernal grid = (GridKernal)grid(i % NODES_CNT);

            GridCacheAdapter<Integer, Integer> cache = grid.internalCache("replicated");

            assert !cache.context().mvcc().remoteCandidates().isEmpty();

            for (int j = 0; j < KEYS_CNT; j++)
                if ((j % NODES_CNT) != (i % NODES_CNT))
                    assert cache.isLocked(j) : "Key should be locked [nodeIdx=" + i + ", key=" + j + "]";
        }
    }

    /** @throws Exception In failed. */
    public void testDgcPartitionedTx() throws Exception {
        GridCacheContext<Integer, Integer> cctx =
            ((GridKernal)grid(0)).<Integer, Integer>internalCache("partitioned").context();

        long topVer = cctx.discovery().topologyVersion();

        for (int key = 0; key < KEYS_CNT; key++) {
            // Resolve primary node for key.
            GridNode primaryNode = F.first(cctx.affinity().nodes(new Integer(key), topVer));

            assert primaryNode != null;

            GridKernal grid = (GridKernal)G.grid(primaryNode.id());

            GridNearCache<Integer, Integer> cache =
                (GridNearCache<Integer, Integer>)grid.<Integer, Integer>internalCache("partitioned");

            // Put key/value to primary and backup nodes.
            cache.put(key, key);
        }

        info("Finished data population.");

        Thread.sleep(DGC_SUSPECT_LOCK_TIMEOUT);

        GridCacheAdapter<Integer, Integer> cache = ((GridKernal)grid(0)).internalCache("partitioned");

        cache.dgc(DGC_SUSPECT_LOCK_TIMEOUT, true, false);

        Thread.sleep(2000 * NODES_CNT);

        checkPartitionedLocks();
    }

    /**
     *
     */
    private void checkPartitionedLocks() {
        info("Checking locks.");

        for (int i = 0; i < NODES_CNT; i++) {
            GridKernal grid = (GridKernal)grid(i);

            GridNearCache<Integer, Integer> cache =
                (GridNearCache<Integer, Integer>)grid.<Integer, Integer>internalCache("partitioned");

            assert !cache.dht().context().mvcc().remoteCandidates().isEmpty();
        }
    }

    /**
     *
     */
    private class TestCommunicationSpi extends GridTcpCommunicationSpi {
        /** */
        private final Collection<GridBiTuple<GridNode, GridTcpCommunicationMessageAdapter>> delayedMsgs =
            new LinkedList<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(GridNode node, GridTcpCommunicationMessageAdapter msg)
            throws GridSpiException {
            if (isTxFinish((GridIoMessage)msg)) {
                if (log.isDebugEnabled())
                    log.debug("Delayed message send: " + msg);

                delayedMsgs.add(F.<GridNode, GridTcpCommunicationMessageAdapter>t(node, msg));

                return;
            }

            super.sendMessage(node, msg);
        }

        /** @throws GridSpiException If sending failed. */
        public void sendDelayedMessages() throws GridSpiException {
            for (GridBiTuple<GridNode, GridTcpCommunicationMessageAdapter> t : delayedMsgs)
                super.sendMessage(t.get1(), t.get2());

            delayedMsgs.clear();
        }

        /**
         * @param msg Message.
         * @return {@code True} if message is tx finish request.
         */
        private boolean isTxFinish(GridIoMessage msg) {
            return msg.message() instanceof GridDistributedTxFinishRequest;
        }
    }
}
