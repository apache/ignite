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

package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestDelayingCommunicationSpi;
import org.apache.ignite.internal.TestDelayingTcpDiscoverySpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.junit.Test;

/**
 * Tests the local affinity recalculation exchange in case of leaving a baseline node.
 */
public class IgniteBaselineNodeLeaveExchangeTest extends GridCommonAbstractTest {
    /** Grids count. */
    private static final int GRIDS_COUNT = 8;

    /** Leaving grid index. */
    private static final int LEAVING_GRID = GRIDS_COUNT / 2;

    /** Tx cache name. */
    private static final String TX_CACHE_NAME = "e"; // group ID = 101

    /** Atomic cache name. */
    private static final String ATOMIC_CACHE_NAME = "f"; // group ID = 102

    /** Test attribute. */
    private static final String TEST_ATTRIBUTE = "testAttribute";

    /** Client grid name. */
    private static final String CLIENT_GRID_NAME = "client";

    /** Dummy grid name. */
    private static final String DUMMY_GRID_NAME = "dummy";

    /** Stop tx load flag. */
    private static final AtomicBoolean txStop = new AtomicBoolean();

    /** Stop atomic load flag. */
    private static final AtomicBoolean atomicStop = new AtomicBoolean();

    /** Keys count. */
    private static final int KEYS_CNT = 100;

    /** Account value bound. */
    private static final long ACCOUNT_VAL_BOUND = 1000;

    /** Account value origin. */
    private static final long ACCOUNT_VAL_ORIGIN = 100;

    /** Blocking discovery spi enabled. */
    private static final AtomicBoolean delayMsg = new AtomicBoolean();

    /** */
    private volatile PickKeyOption pickKeyOption = PickKeyOption.NO_DATA_ON_LEAVING_NODE;

    /** */
    private volatile List<Integer> txKeys;

    /** */
    private volatile List<Integer> atomicKeys;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(300L * 1024 * 1024)
                        .setMaxSize(300L * 1024 * 1024)
                )
                .setWalSegmentSize(1024 * 1024)
        );

        if (igniteInstanceName.contains(DUMMY_GRID_NAME))
            cfg.setUserAttributes(F.asMap(TEST_ATTRIBUTE, false));
        else
            cfg.setUserAttributes(F.asMap(TEST_ATTRIBUTE, true));

        if (igniteInstanceName.contains(CLIENT_GRID_NAME))
            cfg.setClientMode(true);

        CacheConfiguration<Integer, Long> txCfg = new CacheConfiguration<Integer, Long>()
            .setName(TX_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setBackups(2)
            .setNodeFilter(new TestNodeFilter())
            .setAffinity(new RendezvousAffinityFunction(false, 32));

        CacheConfiguration<Integer, Long> atomicCfg = new CacheConfiguration<Integer, Long>()
            .setName(ATOMIC_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(2)
            .setNodeFilter(new TestNodeFilter())
            .setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(txCfg, atomicCfg);

        cfg.setDiscoverySpi(new TestDelayingTcpDiscoverySpi() {
            /** {@inheritDoc} */
            @Override protected boolean delayMessage(TcpDiscoveryAbstractMessage msg) {
                return delayMsg.get() &&
                    (msg instanceof TcpDiscoveryNodeFailedMessage || msg instanceof TcpDiscoveryNodeLeftMessage);
            }
        });

        cfg.setCommunicationSpi(new TestDelayingCommunicationSpi() {
            /** {@inheritDoc} */
            @Override protected boolean delayMessage(Message msg, GridIoMessage ioMsg) {
                return delayMsg.get() &&
                    (msg instanceof GridDhtPartitionsFullMessage || msg instanceof GridDhtPartitionsSingleMessage);
            }

            /** {@inheritDoc} */
            @Override protected int delayMillis() {
                return 1000;
            }
        });

        cfg.setConsistentId(igniteInstanceName);

        cfg.setFailureDetectionTimeout(60_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanup();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanup();
    }

    /**
     * @param dummyCrd Dummy grid coordinator.
     * @throws Exception if failed.
     */
    private void startAndActivateNodes(boolean dummyCrd) throws Exception {
        if (dummyCrd) {
            startGrid(DUMMY_GRID_NAME);

            for (int i = 0; i < GRIDS_COUNT; i++)
                startGrid(i);
        }
        else {
            for (int i = 0; i < GRIDS_COUNT; i++)
                startGrid(i);

            startGrid(DUMMY_GRID_NAME);
        }

        startGrid(CLIENT_GRID_NAME);

        grid(0).cluster().active(true);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanup();
    }

    /**
     *
     */
    private void cleanup() throws Exception {
        delayMsg.set(false);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testServerLeaveOperationsFromClient() throws Exception {
        testUpdatesNotBlockExchangeOnServerLeave(CLIENT_GRID_NAME, true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testServerLeaveOperationsFromDummyCrd() throws Exception {
        testUpdatesNotBlockExchangeOnServerLeave(DUMMY_GRID_NAME, true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testServerLeaveOperationsFromRegularCrd() throws Exception {
        testUpdatesNotBlockExchangeOnServerLeave(DUMMY_GRID_NAME, false);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testServerLeaveOperationsFromServer() throws Exception {
        testUpdatesNotBlockExchangeOnServerLeave(getTestIgniteInstanceName(0), true);
    }

    /**
     * @param operationsGridName Grid name.
     * @param dummyCrd Dummy grid coordinator.
     * @throws Exception if failed.
     */
    private void testUpdatesNotBlockExchangeOnServerLeave(String operationsGridName,
        boolean dummyCrd) throws Exception {
        startAndActivateNodes(dummyCrd);

        populateData(grid(operationsGridName), TX_CACHE_NAME);

        putGetSelfCheck(operationsGridName);

        IgniteInternalCache<Integer, Long> cachex = grid(LEAVING_GRID).cachex(TX_CACHE_NAME);

        ArrayList<Integer> backupKeys = pickKey(cachex.context(), PickKeyOption.BACKUP_ON_LEAVING_NODE);

        assertFalse(backupKeys.isEmpty());

        CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            IgniteEx ignite = grid(operationsGridName);

            IgniteInternalCache<Integer, Long> cache = ignite.cachex(TX_CACHE_NAME);

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
                cache.put(backupKeys.get(rnd.nextInt(backupKeys.size())), rnd.nextLong());

                assertTrue("Exchange waited for partition released.", latch.await(30, TimeUnit.SECONDS));

                tx.commit();
            }

            return true;
        });

        stopGrid(LEAVING_GRID);

        awaitPartitionMapExchange();

        latch.countDown();

        fut.get();

        putGetSelfCheck(operationsGridName);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testServerLeaveUnderLoadFromClient() throws Exception {
        testBltServerLeaveUnderLoad(CLIENT_GRID_NAME, true, pickKeyOption);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testServerLeaveUnderLoadFromDummyCrd() throws Exception {
        testBltServerLeaveUnderLoad(DUMMY_GRID_NAME, true, pickKeyOption);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testServerLeaveUnderLoadFromRegularCrd() throws Exception {
        testBltServerLeaveUnderLoad(DUMMY_GRID_NAME, false, pickKeyOption);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testServerLeaveUnderLoadFromFirstServer() throws Exception {
        testBltServerLeaveUnderLoad(getTestIgniteInstanceName(0), true, pickKeyOption);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testServerLeaveUnderLoadFromLastServer() throws Exception {
        testBltServerLeaveUnderLoad(getTestIgniteInstanceName(GRIDS_COUNT - 1), true, pickKeyOption);
    }

    /**
     * @param operationsGridName Grid name.
     * @param dummyCrd Dummy grid coordinator.
     * @param pickKeyOption Pick key option.
     * @throws Exception if failed.
     */
    private void testBltServerLeaveUnderLoad(
        String operationsGridName,
        boolean dummyCrd,
        PickKeyOption pickKeyOption
    ) throws Exception {
        startAndActivateNodes(dummyCrd);

        populateData(grid(operationsGridName), TX_CACHE_NAME);
        populateData(grid(operationsGridName), ATOMIC_CACHE_NAME);

        putGetSelfCheck(operationsGridName);

        IgniteInternalCache<Integer, Long> txCache = grid(LEAVING_GRID).cachex(TX_CACHE_NAME);
        IgniteInternalCache<Integer, Long> aCache = grid(LEAVING_GRID).cachex(ATOMIC_CACHE_NAME);

        txKeys = pickKey(txCache.context(), pickKeyOption);
        atomicKeys = pickKey(aCache.context(), pickKeyOption);

        assertFalse(txKeys.isEmpty());
        assertFalse(atomicKeys.isEmpty());

        GridCompoundFuture<Long, Void> fut = new GridCompoundFuture<>();

        fut.add(startTxLoad(1, operationsGridName, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, pickKeyOption));
        fut.add(startTxLoad(1, operationsGridName, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED, pickKeyOption));
        fut.add(startTxLoad(1, operationsGridName, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE, pickKeyOption));
        fut.add(startAtomicLoad(1, operationsGridName, pickKeyOption));

        fut.markInitialized();

        U.sleep(1000);

        delayMsg.set(true);

        stopGrid(LEAVING_GRID, true);

        awaitPartitionMapExchange();

        delayMsg.set(false);

        U.sleep(5000);

        txStop.set(true);
        atomicStop.set(true);

        fut.get();

        putGetSelfCheck(operationsGridName);
    }

    /**
     * Dumps cache state as map.
     *
     * @param cache Cache.
     * @return Entries of cache in map.
     */
    protected Map<Integer, Long> dumpCache(IgniteCache<Integer, Long> cache) {
        Map<Integer, Long> map = new HashMap<>(KEYS_CNT);

        for (Cache.Entry<Integer, Long> e : cache)
            map.put(e.getKey(), e.getValue());

        return map;
    }

    /**
     * @param checkNodeName Check node name.
     */
    private void putGetSelfCheck(String checkNodeName) {
        IgniteCache<Integer, Long> txCache = grid(checkNodeName).cache(TX_CACHE_NAME);
        IgniteCache<Integer, Long> atomicCache = grid(checkNodeName).cache(ATOMIC_CACHE_NAME);

        Map<Integer, Long> txDump = dumpCache(txCache);
        Map<Integer, Long> atomicDump = dumpCache(atomicCache);

        for (Map.Entry<Integer, Long> entry : txDump.entrySet())
            assertEquals(entry.getValue(), txCache.get(entry.getKey()));

        for (Map.Entry<Integer, Long> entry : atomicDump.entrySet())
            assertEquals(entry.getValue(), atomicCache.get(entry.getKey()));
    }

    /**
     * @param threads Threads.
     * @param loadGridName Load grid name.
     * @param txConc Tx conc.
     * @param txIs Tx is.
     */
    @SuppressWarnings({"StatementWithEmptyBody"})
    private IgniteInternalFuture<Long> startTxLoad(
        int threads,
        String loadGridName,
        TransactionConcurrency txConc,
        TransactionIsolation txIs,
        PickKeyOption pickKeyOption
    ) {
        txStop.set(false);

        AtomicLong lastCommitTs = new AtomicLong(Long.MAX_VALUE);
        AtomicLong maxSla = new AtomicLong(0);
        AtomicLong maxSlaAwakenTs = new AtomicLong(0);

        IgniteInternalFuture<Long> resFut = GridTestUtils.runMultiThreadedAsync(() -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!txStop.get()) {
                    IgniteEx ignite = grid(loadGridName);

                    IgniteCache<Integer, Long> cache = ignite.cache(TX_CACHE_NAME);

                    try (Transaction tx = ignite.transactions().txStart(txConc, txIs)) {
                        int acc0 = txKeys.get(rnd.nextInt(txKeys.size()));

                        int acc1;

                        while ((acc1 = txKeys.get(rnd.nextInt(txKeys.size()))) == acc0)
                            ;

                        // Avoid deadlocks.
                        if (acc0 > acc1) {
                            int tmp = acc0;
                            acc0 = acc1;
                            acc1 = tmp;
                        }

                        long val0 = cache.get(acc0);
                        long val1 = cache.get(acc1);

                        long delta = rnd.nextLong(Math.max(val0, val1));

                        if (val0 < val1) {
                            cache.put(acc0, val0 + delta);
                            cache.put(acc1, val1 - delta);
                        }
                        else {
                            cache.put(acc0, val0 - delta);
                            cache.put(acc1, val1 + delta);
                        }

                        tx.commit();

                        long now = System.currentTimeMillis();
                        long last = lastCommitTs.getAndSet(now);

                        if (last < now) {
                            long max = maxSla.get();

                            if (max < now - last) {
                                maxSla.set(now - last);

                                maxSlaAwakenTs.set(now);
                            }
                        }
                    }
                    catch (TransactionOptimisticException ignored) {
                        // No-op.
                    }
                    catch (Exception e) {
                        if (X.hasCause(e, ClusterTopologyException.class, ClusterTopologyCheckedException.class))
                            U.warn(log, "User transaction resulted in topology exception", e);
                        else
                            throw e;
                    }
                }
            },
            threads, "tx-load-thread-" + loadGridName + "-" + txConc.toString() + "-" + txIs.toString()
        );

        resFut.listen(fut -> log.info("Max SLA for tx load config " +
            "[threads=" + threads +
            ",loadGridName=" + loadGridName +
            ",txConc=" + txConc +
            ",txIs=" + txIs +
            ",pickKeyOption=" + pickKeyOption
            + "] is " + maxSla.get() + "ms, " +
            "awaken from max SLA hang " + (System.currentTimeMillis() - maxSlaAwakenTs.get()) + "ms ago."));

        return resFut;
    }

    /**
     * Picks key for cache operation, optionally enforces or avoids selecting key that is present on {@link
     * #LEAVING_GRID}.
     *
     * @param cctx Cctx.
     * @param pickKeyOption Pick key option.
     * @return Array of keys.
     */
    private ArrayList<Integer> pickKey(GridCacheContext<Integer, ?> cctx, PickKeyOption pickKeyOption) {
        ArrayList<Integer> keys = new ArrayList<>();

        List<List<ClusterNode>> idealAssignment = cctx.affinity().idealAssignment();

        AffinityFunction aff = cctx.config().getAffinity();

        String leaveNodeName = getTestIgniteInstanceName(LEAVING_GRID);

        for (int key = 0; key < KEYS_CNT; key++) {
            int part = aff.partition(key);

            List<ClusterNode> nodes = idealAssignment.get(part);

            switch (pickKeyOption) {
                case PRIMARY_ON_LEAVING_NODE:
                    if (leaveNodeName.equals(nodes.get(0).consistentId()))
                        keys.add(key);

                    break;

                case BACKUP_ON_LEAVING_NODE:
                    for (ClusterNode node : nodes.subList(1, nodes.size())) {
                        if (leaveNodeName.equals(node.consistentId()))
                            keys.add(key);
                    }

                    break;

                case NO_DATA_ON_LEAVING_NODE:
                    boolean needRetry = false;

                    for (ClusterNode node : nodes) {
                        if (leaveNodeName.equals(node.consistentId()))
                            needRetry = true;
                    }

                    if (!needRetry)
                        keys.add(key);

                    break;

                case ANY:
                    keys.add(key);

                    break;
            }
        }

        return keys;
    }

    /**
     * @param threads Threads.
     * @param loadGridName Load grid name.
     * @param pickKeyOption Pick key option.
     */
    protected IgniteInternalFuture<Long> startAtomicLoad(
        int threads,
        String loadGridName,
        PickKeyOption pickKeyOption
    ) {
        atomicStop.set(false);

        AtomicLong lastCommitTs = new AtomicLong(Long.MAX_VALUE);
        AtomicLong maxSla = new AtomicLong(0);
        AtomicLong maxSlaAwakenTs = new AtomicLong(0);

        IgniteInternalFuture<Long> resFut = GridTestUtils.runMultiThreadedAsync(() -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!atomicStop.get()) {
                    IgniteEx ignite = grid(loadGridName);

                    IgniteCache<Integer, Long> cache = ignite.cache(ATOMIC_CACHE_NAME);

                    int cnt = rnd.nextInt(KEYS_CNT / 10);

                    try {
                        for (int i = 0; i < cnt; i++) {
                            cache.put(atomicKeys.get(rnd.nextInt(atomicKeys.size())),
                                rnd.nextLong(ACCOUNT_VAL_ORIGIN, ACCOUNT_VAL_BOUND + 1));
                        }

                        long now = System.currentTimeMillis();
                        long last = lastCommitTs.getAndSet(now);

                        if (last < now) {
                            long max = maxSla.get();

                            if (max < now - last) {
                                maxSla.set(now - last);

                                maxSlaAwakenTs.set(now);
                            }
                        }
                    }
                    catch (IgniteException e) {
                        U.error(log, "User atomic put failed", e);
                    }
                }
            },
            threads, "atomic-load-thread"
        );

        resFut.listen(fut -> log.info("Max SLA for atomic load config " +
            "[threads=" + threads +
            ", loadGridName=" + loadGridName +
            ", pickKeyOption=" + pickKeyOption
            + "] is " + maxSla.get() + "ms, " +
            "awaken from max SLA hang " + (System.currentTimeMillis() - maxSlaAwakenTs.get()) + "ms ago."));

        return resFut;
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @return Sum of all cache values.
     */
    protected long populateData(Ignite ignite, String cacheName) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long total = 0;

        try (IgniteDataStreamer<Integer, Long> dataStreamer = ignite.dataStreamer(cacheName)) {
            dataStreamer.allowOverwrite(false);

            for (int i = 0; i < KEYS_CNT; i++) {
                long val = rnd.nextLong(ACCOUNT_VAL_ORIGIN, ACCOUNT_VAL_BOUND + 1);

                dataStreamer.addData(i, val);

                total += val;
            }

            dataStreamer.flush();
        }

        log.info("Total sum for cache '" + cacheName + "': " + total);

        return total;
    }

    /**
     * @param cache Cache.
     */
    protected long sumOf(IgniteCache<Integer, Long> cache) {
        long sum = 0;

        for (int i = 0; i < KEYS_CNT; i++)
            sum += cache.get(i);

        return sum;
    }

    /** */
    private static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return Boolean.TRUE.equals(clusterNode.attribute(TEST_ATTRIBUTE));
        }
    }

    /**
     *
     */
    private enum PickKeyOption {
        /** Key will be selected randomly. */ ANY,

        /** Key's primary partition will reside on leaving node. */ PRIMARY_ON_LEAVING_NODE,

        /** Key's backup partition will reside on leaving node. */ BACKUP_ON_LEAVING_NODE,

        /** All partitions that contain key will reside anywhere except leaving node. */ NO_DATA_ON_LEAVING_NODE
    }
}
