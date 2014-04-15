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
import org.gridgain.grid.kernal.processors.cache.distributed.dht.colocated.*;
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

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;

/**
 * Cache DGC test.
 */
public class GridCacheDgcManagerTxOnDemandSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 4;

    /** */
    private static final int KEYS_CNT = NODES_CNT * 5;

    /** */
    private static final int DGC_FREQ = 0;

    /** */
    private static final int DGC_SUSPECT_LOCK_TIMEOUT = 1000;

    /** */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

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
        repCacheCfg.setDefaultTxConcurrency(PESSIMISTIC);
        repCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_ASYNC);
        repCacheCfg.setEvictionPolicy(null);

        GridCacheConfiguration partCacheCfg = defaultCacheConfiguration();

        partCacheCfg.setName("partitioned");

        partCacheCfg.setCacheMode(PARTITIONED);
        partCacheCfg.setBackups(1);
        partCacheCfg.setPreloadMode(SYNC);
        partCacheCfg.setDgcFrequency(DGC_FREQ);
        partCacheCfg.setDgcSuspectLockTimeout(DGC_SUSPECT_LOCK_TIMEOUT);
        partCacheCfg.setDefaultTxConcurrency(PESSIMISTIC);
        partCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_ASYNC);
        partCacheCfg.setEvictionPolicy(null);
        partCacheCfg.setNearEvictionPolicy(null);
        partCacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(repCacheCfg, partCacheCfg);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        // Override communication SPI.
        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /** @throws Exception In failed. */
    public void testDgcReplicatedTx() throws Exception {
        for (int key = 0; key < KEYS_CNT; key++) {
            GridKernal grid = (GridKernal)grid(key % NODES_CNT);

            GridCacheAdapter<Integer, Integer> cache = grid.internalCache("replicated");

            // Put key/value to all nodes.
            cache.put(key, key);
        }

        info("Finished data population. Going to sleep...");

        GridCache<Integer, Integer> cache = grid(0).cache("replicated");

        Thread.sleep(DGC_SUSPECT_LOCK_TIMEOUT * 2);

        cache.dgc();
        cache.dgc();
        cache.dgc();

        Thread.sleep(5000 * NODES_CNT);

        checkReplicatedLocks();
    }

    /**
     *
     */
    private void checkReplicatedLocks() {
        info("Checking locks.");

        for (int i = 0; i < KEYS_CNT; i++) {
            GridKernal grid = (GridKernal)grid(i % NODES_CNT);

            GridCacheAdapter<Integer, Integer> cache = grid.internalCache("replicated");

            assert cache.context().mvcc().remoteCandidates().isEmpty();

            for (int j = 0; j < KEYS_CNT; j++)
                assert !cache.isLocked(j) : "Key should not be locked [nodeIdx=" + i + ", key=" + j + "]";
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

            GridCache<Integer, Integer> cache =
                grid.cache("partitioned");

            // Put key/value to primary and backup nodes.
            cache.put(key, key);
        }

        info("Finished data population. Going to sleep...");

        // We cannot ensure that entries are locked properly on all nodes,
        // since locks might have already been removed by DGC.

        GridCacheAdapter<Integer, Integer> cache = ((GridKernal)grid(0)).internalCache("partitioned");

        Thread.sleep(DGC_SUSPECT_LOCK_TIMEOUT * 2);

        cache.dgc();
        cache.dgc();
        cache.dgc();

        Thread.sleep(5000 * NODES_CNT);

        checkPartitionedLocks();
    }

    /**
     *
     */
    private void checkPartitionedLocks() {
        info("Checking locks.");

        for (int i = 0; i < NODES_CNT; i++) {
            GridKernal grid = (GridKernal)grid(i);

            if (isNearEnabled(grid.internalCache("partitioned").configuration())) {
                GridNearCacheAdapter<Integer, Integer> cache =
                    (GridNearCacheAdapter<Integer, Integer>)grid.<Integer, Integer>internalCache("partitioned");

                assert cache.context().mvcc().remoteCandidates().isEmpty() :
                    cache.context().mvcc().remoteCandidates();
                assert cache.dht().context().mvcc().remoteCandidates().isEmpty() :
                    cache.dht().context().mvcc().remoteCandidates();

                for (int j = 0; j < KEYS_CNT; j++) {
                    assert !cache.isLocked(j) : "Key should not be locked in near [nodeIdx=" + i + ", key=" + j + "]";
                    assert !cache.dht().isLocked(j) : "Key should not be locked in dht [nodeIdx=" + i + ", key=" + j
                        + "]";
                }
            }
            else {
                GridDhtColocatedCache<Integer, Integer> cache =
                    (GridDhtColocatedCache<Integer, Integer>)grid.<Integer, Integer>internalCache("partitioned");

                assert cache.context().mvcc().remoteCandidates().isEmpty() :
                    cache.context().mvcc().remoteCandidates();

                for (int j = 0; j < KEYS_CNT; j++) {
                    try {
                        GridCacheEntryEx<Integer, Integer> entry = cache.peekEx(j);

                        assert entry == null || !entry.lockedByAny() : "Key should not be locked in colocated " +
                            "[nodeIdx=" + i + ", key=" + j + "]";
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        // No-op.
                    }
                }
            }
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
        @Override public void sendMessage(GridNode node, GridTcpCommunicationMessageAdapter msg) throws GridSpiException {
            if (isTxFinish(msg)) {
                if (log.isDebugEnabled())
                    log.debug("Delayed message send: " + msg);

                delayedMsgs.add(F.t(node, msg));

                return;
            }

            super.sendMessage(node, msg);
        }

        /**
         * @throws GridSpiException If sending failed.
         */
        public void sendDelayedMessages() throws GridSpiException {
            for (GridBiTuple<GridNode, GridTcpCommunicationMessageAdapter> t : delayedMsgs)
                sendMessage(t.get1(), t.get2());

            delayedMsgs.clear();
        }

        /**
         * @param msg Message.
         * @return {@code True} if message is tx finish request.
         */
        private boolean isTxFinish(GridTcpCommunicationMessageAdapter msg) {
            return ((GridIoMessage)msg).message() instanceof GridDistributedTxFinishRequest;
        }
    }
}
