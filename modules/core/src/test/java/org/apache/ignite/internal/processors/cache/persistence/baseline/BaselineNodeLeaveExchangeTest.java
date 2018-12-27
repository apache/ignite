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

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;

/**
 *
 */
public class BaselineNodeLeaveExchangeTest extends GridCommonAbstractTest {
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
    private static final int KEYS_CNT = 50;

    /** Account value bound. */
    private static final long ACCOUNT_VAL_BOUND = 1000;

    /** Account value origin. */
    private static final long ACCOUNT_VAL_ORIGIN = 100;

    /** Blocking discovery spi enabled. */
    private static final AtomicBoolean blockingDiscoverySpiEnabled = new AtomicBoolean();

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

        cfg.setDiscoverySpi(new BlockingDiscoverySpi());

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
     *
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
        blockingDiscoverySpiEnabled.set(false);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    public void testServerLeaveOperationsFromClient() throws Exception {
        testServerLeave(CLIENT_GRID_NAME, true);
    }

    /**
     *
     */
    public void testServerLeaveOperationsFromDummyCrd() throws Exception {
        testServerLeave(DUMMY_GRID_NAME, true);
    }

    /**
     *
     */
    public void testServerLeaveOperationsFromRegularCrd() throws Exception {
        testServerLeave(DUMMY_GRID_NAME, false);
    }

    /**
     *
     */
    public void testServerLeaveOperationsFromServer() throws Exception {
        testServerLeave(getTestIgniteInstanceName(0), true);
    }

    /**
     *
     */
    private void testServerLeave(String operationsGridName, boolean dummyCrd) throws Exception {
        startAndActivateNodes(dummyCrd);

        populateData(grid(operationsGridName), TX_CACHE_NAME);
        populateData(grid(operationsGridName), ATOMIC_CACHE_NAME);

        putGetSelfCheck(operationsGridName);

        stopGrid(LEAVING_GRID);

        awaitPartitionMapExchange();

        atomicStop.set(true);
        txStop.set(true);

        putGetSelfCheck(operationsGridName);
    }

    /**
     *
     */
    public void testServerLeaveUnderLoadFromClient() throws Exception {
        testBltServerLeaveUnderLoad(CLIENT_GRID_NAME, true, PickKeyOption.NO_DATA_ON_LEAVING_NODE);
    }

    /**
     *
     */
    public void testServerLeaveUnderLoadFromDummyCrd() throws Exception {
        testBltServerLeaveUnderLoad(DUMMY_GRID_NAME, true, PickKeyOption.NO_DATA_ON_LEAVING_NODE);
    }

    /**
     *
     */
    public void testServerLeaveUnderLoadFromRegularCrd() throws Exception {
        testBltServerLeaveUnderLoad(DUMMY_GRID_NAME, false, PickKeyOption.NO_DATA_ON_LEAVING_NODE);
    }

    /**
     *
     */
    public void testServerLeaveUnderLoadFromFirstServer() throws Exception {
        testBltServerLeaveUnderLoad(getTestIgniteInstanceName(0), true, PickKeyOption.NO_DATA_ON_LEAVING_NODE);
    }

    /**
     *
     */
    public void testServerLeaveUnderLoadFromLastServer() throws Exception {
        testBltServerLeaveUnderLoad(getTestIgniteInstanceName(GRIDS_COUNT - 1), true, PickKeyOption.NO_DATA_ON_LEAVING_NODE);
    }

    /**
     *
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

        GridCompoundFuture<Long, Void> fut = new GridCompoundFuture<>();

        fut.add(startTxLoad(1, operationsGridName, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, pickKeyOption));
        fut.add(startTxLoad(1, operationsGridName, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED, pickKeyOption));
        fut.add(startTxLoad(1, operationsGridName, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE, pickKeyOption));
        fut.add(startAtomicLoad(1, operationsGridName, pickKeyOption));

        fut.markInitialized();
        
        U.sleep(1000);

        blockingDiscoverySpiEnabled.set(true);

        log.info("@@@ stopping node");

        stopGrid(LEAVING_GRID);

        awaitPartitionMapExchange();

        log.info("@@@ awaitPartitionMapExchange() ended");

        blockingDiscoverySpiEnabled.set(false);

        U.sleep(5000);

        atomicStop.set(true);
        txStop.set(true);
        
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
                    IgniteInternalCache<Integer, Long> cachex = ignite.cachex(TX_CACHE_NAME);

                    try (Transaction tx = ignite.transactions().txStart(txConc, txIs)) {
                        int acc0 = pickKey(cachex.context(), pickKeyOption);

                        int acc1;

                        while ((acc1 = pickKey(cachex.context(), pickKeyOption)) == acc0)
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
                    catch (CacheException e) {
                        if (X.hasCause(e, ClusterTopologyException.class))
                            U.warn(log, "User transaction resulted in topology exception", e);
                    }
                    catch (IgniteException e) {
                        U.error(log, "User transaction failed", e);
                    }
                }
            },
            threads, "tx-load-thread-" + loadGridName + "-" + txConc.toString() + "-" + txIs.toString()
        );
        
        resFut.listen(fut -> log.info("@@@ Max SLA for tx load config " +
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
     * Picks key for cache operation, optionally enforces or avoids selecting key
     * that is present on {@link #LEAVING_GRID}.
     *
     * @param cctx Cctx.
     * @param pickKeyOption Pick key option.
     */
    private int pickKey(GridCacheContext<Integer, ?> cctx, PickKeyOption pickKeyOption) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (true) {
            int res = rnd.nextInt(KEYS_CNT);

            if (pickKeyOption == PickKeyOption.ANY)
                return res;

            List<List<ClusterNode>> idealAssignment = cctx.affinity().idealAssignment();

            AffinityFunction aff = cctx.config().getAffinity();

            int part = aff.partition(res);

            List<ClusterNode> nodes = idealAssignment.get(part);

            if (pickKeyOption == PickKeyOption.NO_DATA_ON_LEAVING_NODE) {
                boolean needRetry = false;

                for (ClusterNode n : nodes) {
                    if (n.consistentId().equals(getTestIgniteInstanceName(LEAVING_GRID)))
                        needRetry = true;
                }

                if (!needRetry)
                    return res;
            }
            else if (pickKeyOption == PickKeyOption.PRIMARY_ON_LEAVING_NODE) {
                if (nodes.get(0).consistentId().equals(getTestIgniteInstanceName(LEAVING_GRID)))
                    return res;
            }
            else if (pickKeyOption == PickKeyOption.BACKUP_ON_LEAVING_NODE) {
                for (ClusterNode n : nodes.subList(1, nodes.size())) {
                    if (n.consistentId().equals(getTestIgniteInstanceName(LEAVING_GRID)))
                        return res;
                }
            }
            else
                throw new IllegalArgumentException("Unexpected pick key option: " + pickKeyOption);
        }
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

                    IgniteInternalCache<Integer, Long> cachex = ignite.cachex(ATOMIC_CACHE_NAME);
                    IgniteCache<Integer, Long> cache = ignite.cache(ATOMIC_CACHE_NAME);

                    int cnt = rnd.nextInt(KEYS_CNT / 10);

                    try {
                        for (int i = 0; i < cnt; i++) {
                            cache.put(pickKey(cachex.context(), pickKeyOption),
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

        resFut.listen(fut -> log.info("@@@ Max SLA for atomic load config " +
            "[threads=" + threads +
            ",loadGridName=" + loadGridName +
            ",pickKeyOption=" + pickKeyOption
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
    private static class BlockingDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            holdMessageIfNeeded(msg);

            super.writeToSocket(msg, sock, res, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            holdMessageIfNeeded(msg);

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            holdMessageIfNeeded(msg);

            super.writeToSocket(sock, out, msg, timeout);
        }

        /**
         * @param msg Message.
         */
        private void holdMessageIfNeeded(TcpDiscoveryAbstractMessage msg) {
            if (blockingDiscoverySpiEnabled.get() &&
                (msg instanceof TcpDiscoveryNodeFailedMessage || msg instanceof TcpDiscoveryNodeLeftMessage)) {
                try {
                    U.warn(log, "Holding message a bit: " + msg.toString());

                    U.sleep(2_000);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // No-op.
                }
            }
        }
    }

    /**
     *
     */
    private enum PickKeyOption {
        /** Key will be selected randomly. */ANY,

        /** Key's primary partition will reside on leaving node. */PRIMARY_ON_LEAVING_NODE,

        /** Key's backup partition will reside on leaving node. */BACKUP_ON_LEAVING_NODE,

        /** All partitions that contain key will reside anywhere except leaving node. */NO_DATA_ON_LEAVING_NODE
    }
}
