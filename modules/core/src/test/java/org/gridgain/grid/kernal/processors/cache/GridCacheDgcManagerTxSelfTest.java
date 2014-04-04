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
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
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
import org.jdk8.backport.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;

/**
 * Cache DGC test.
 */
public class GridCacheDgcManagerTxSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 4;

    /** */
    private static final int KEY_CNT = NODES_CNT * 5;

    /** */
    private static final int DGC_FREQ = 5000;

    /** */
    private static final int DGC_SUSPECT_LOCK_TIMEOUT = 1000;

    /** */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final Collection<TestCommunicationSpi> commSpis = new ConcurrentLinkedDeque8<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_CNT);

        for (int i = 0; i < NODES_CNT; i++) {
            GridKernal kernal = (GridKernal)grid(i);

            kernal.internalCache("replicated").context().tm().finishSyncDisabled(true);
            kernal.internalCache("partitioned").context().tm().finishSyncDisabled(true);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        commSpis.clear();
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
        repCacheCfg.setWriteSynchronizationMode(FULL_ASYNC);

        GridCacheConfiguration partCacheCfg = defaultCacheConfiguration();

        partCacheCfg.setName("partitioned");

        partCacheCfg.setCacheMode(PARTITIONED);
        partCacheCfg.setPreloadMode(SYNC);
        partCacheCfg.setDgcFrequency(DGC_FREQ);
        partCacheCfg.setDgcSuspectLockTimeout(DGC_SUSPECT_LOCK_TIMEOUT);
        partCacheCfg.setDefaultTxConcurrency(PESSIMISTIC);
        partCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_ASYNC);
        partCacheCfg.setAtomicityMode(TRANSACTIONAL);
        partCacheCfg.setBackups(1);

        cfg.setCacheConfiguration(repCacheCfg, partCacheCfg);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        // Override communication SPI.
        TestCommunicationSpi commSpi = new TestCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        commSpis.add(commSpi);

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testDgcReplicatedOneTx() throws Exception {
        checkDgcReplicatedTx(false, 1);
    }

    /** @throws Exception If failed. */
    public void testDgcReplicatedManyTx() throws Exception {
        checkDgcReplicatedTx(true, KEY_CNT);
    }

    /**
     * @param rand If {@code true}, then random grid node will be picked for cache population.
     * @param keyCnt Key count.
     * @throws Exception In failed.
     */
    private void checkDgcReplicatedTx(boolean rand, int keyCnt) throws Exception {
        for (int key = 0; key < keyCnt; key++) {
            GridKernal grid = (GridKernal)grid(rand ? key % NODES_CNT : 0);

            GridCache<Integer, Integer> cache = grid.cache("replicated");

            if (key % 2 == 0) {
                // Put key/value to primary and backup nodes.
                cache.put(key, key);
            }
            else {
                GridCacheTx tx = cache.txStart();

                try {
                    cache.put(key, key);
                }
                finally {
                    tx.rollback();
                }
            }
        }

        info("Finished data population. Going to sleep...");

        // We cannot ensure that entries are locked properly on all nodes,
        // since locks might have already been removed by DGC.
        Thread.sleep(DGC_FREQ * NODES_CNT);

        checkReplicatedLocks(keyCnt);

        sendAllDelayedMessages();

        checkReplicatedLocks(keyCnt);
    }

    /** @param keyCnt Key count. */
    private void checkReplicatedLocks(int keyCnt) {
        info("Checking locks.");

        for (int i = 0; i < NODES_CNT; i++) {
            GridKernal grid = (GridKernal)grid(i);

            GridCacheAdapter<Integer, Integer> cache = grid.internalCache("replicated");

            assert cache.context().mvcc().remoteCandidates().isEmpty() : cache.context().mvcc().remoteCandidates();

            for (int j = 0; j < keyCnt; j++)
                assert !cache.isLocked(j) : "Key should not be locked [nodeIdx=" + i + ", key=" + j + "]";
        }
    }

    /** @throws Exception If failed. */
    public void testDgcPartitionedOneTxPrimary() throws Exception {
        checkDgcPartitionedTx(0, 1, false);
    }

    /** @throws Exception If failed. */
    public void testDgcPartitionedOneTxBackup() throws Exception {
        checkDgcPartitionedTx(1, 2, false);
    }

    /** @throws Exception If failed. */
    public void testDgcPartitionedOneTxReader() throws Exception {
        checkDgcPartitionedTx(2, 3, false);
    }

    /** @throws Exception If failed. */
    public void testDgcPartitionedMultiTx() throws Exception {
        checkDgcPartitionedTx(0, KEY_CNT, true);
    }

    /**
     * @param startKey Start key.
     * @param keyCnt Key count.
     * @param rollback Rollback flag.
     * @throws Exception In failed.
     */
    @SuppressWarnings({"IfMayBeConditional", "UnnecessaryBoxing"})
    private void checkDgcPartitionedTx(int startKey, int keyCnt, boolean rollback) throws Exception {
        GridCacheContext<Integer, Integer> cctx =
            ((GridKernal)grid(0)).<Integer, Integer>internalCache("partitioned").context();

        Collection<GridNode> nodes = grid(0).nodes();

        for (int key = startKey; key < keyCnt; key++) {
            // Resolve node to put key to (mix primary, backup and other nodes).
            GridNode node;

            if (key % 3 == 0)
                node = F.first(cctx.affinity().nodes(new Integer(key)));
            else if (key % 3 == 1)
                node = F.first(cctx.affinity().backups(new Integer(key)));
            else
                node = F.first(F.view(nodes, F.notContains(cctx.affinity().nodes(new Integer(key)))));

            assert node != null;

            GridKernal grid = (GridKernal)G.grid(node.id());

            GridCache<Integer, Integer> cache = grid.cache("partitioned");

            if (key % 2 == 0 || !rollback) {
                // Put key/value to primary and backup nodes.
                cache.put(key, key);
            }
            else {
                GridCacheTx tx = cache.txStart();

                try {
                    cache.put(key, key);
                }
                finally {
                    tx.rollback();
                }
            }
        }

        info("Finished data population. Going to sleep...");

        // We cannot ensure that entries are locked properly on all nodes,
        // since locks might have already been removed by DGC.
        Thread.sleep(DGC_FREQ * NODES_CNT);

        checkPartitionedLocks(keyCnt);

        sendAllDelayedMessages();

        Thread.sleep(DGC_FREQ * NODES_CNT);

        checkPartitionedLocks(keyCnt);
    }

    /** @throws Exception If failed. */
    private void sendAllDelayedMessages() throws Exception {
        info("Sending delayed messages.");

        for (TestCommunicationSpi commSpi : commSpis)
            if (commSpi != null)
                commSpi.sendDelayedMessages();
    }

    /** @param keyCnt Key count. */
    private void checkPartitionedLocks(int keyCnt) {
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

                for (int j = 0; j < keyCnt; j++) {
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

                for (int j = 0; j < keyCnt; j++) {
                    try {
                        GridDhtCacheEntry<Integer, Integer> entry = cache.peekExx(j);

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

                delayedMsgs.add(F.<GridNode, GridTcpCommunicationMessageAdapter>t(node, msg));

                return;
            }

            super.sendMessage(node, msg);
        }

        /**
         * @throws GridSpiException If sending failed.
         */
        public void sendDelayedMessages() throws GridSpiException {
            for (GridBiTuple<GridNode, GridTcpCommunicationMessageAdapter> t : delayedMsgs)
                super.sendMessage(t.get1(), t.get2());

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
