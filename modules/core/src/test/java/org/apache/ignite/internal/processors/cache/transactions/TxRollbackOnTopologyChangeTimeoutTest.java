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
package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.util.lang.gridfunc.ReadOnlyCollectionView2X;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests rollback transactions on topology change
 */
public class TxRollbackOnTopologyChangeTimeoutTest extends GridCommonAbstractTest {

    /**  */
    private static final long ROLLBACK_ON_TOPOLOGY_CHANGE_TIMEOUT = 500;

    /**  */
    private static final int TX_TIMEOUT = 1000;

    /**  */
    private static final int TX_COUNT = 100;

    /**  */
    private static final int MAX_BACKUPS = 3;

    /**  */
    private static final int MAX_SRV_NODES = MAX_BACKUPS + 1;

    /**  */
    private static final int CLIENT_IDX = MAX_SRV_NODES;

    /**  */
    private static final int MAX_CLNT_NODES = 3;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**  */
    private static final String TMP_CACHE = "tmpCache";

    /**  */
    private List<Callable<Ignite>> shuffleOps;

    /**  */
    private Collection<Callable<Ignite>> minorOps;

    /**  */
    private AtomicInteger binaryTypeIdx;

    /**  */
    private int backups;

    /**  */
    private CountDownLatch finishTxLatch;

    /**  */
    private CountDownLatch closeTxLatch;

    /**  */
    private Collection<Exception> txErrors;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_START_CACHES_ON_JOIN, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IgniteSystemProperties.IGNITE_START_CACHES_ON_JOIN);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        int idx = getTestIgniteInstanceIndex(igniteInstanceName);

        if (idx >= CLIENT_IDX)
            cfg.setClientMode(true);

        TcpDiscoverySpi disco = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        TransactionConfiguration tcfg = cfg.getTransactionConfiguration();

        tcfg.setRollbackOnTopologyChangeTimeout(ROLLBACK_ON_TOPOLOGY_CHANGE_TIMEOUT);

        CacheConfiguration txCache = defaultCacheConfiguration();

        txCache.setBackups(backups);

        cfg.setCacheConfiguration(txCache);

        return cfg;
    }

    /**  */
    private void startTx() throws Exception {
        final List<Ignite> grids = G.allGrids();

        final AtomicInteger idx = new AtomicInteger(0);

        final Random rnd = new Random();

        int txCount = grids.size() * TX_COUNT;

        final CountDownLatch startTxLatch = new CountDownLatch(txCount);

        final Collection<Exception> errors = new LinkedBlockingQueue<>();

        txErrors = errors;

        finishTxLatch = new CountDownLatch(1);

        closeTxLatch = new CountDownLatch(txCount);

        multithreadedAsync(new Runnable() {

            private boolean txStarted;

            @Override public void run() {
                try {
                    startTx();
                }
                finally {
                    if (!txStarted)
                        fail("Transaction was not started");
                }
            }

            private void startTx() {
                int thIdx = idx.getAndIncrement();

                int gridIdx = thIdx % grids.size();

                Ignite grid = grids.get(gridIdx);

                Thread.currentThread().setName("startTx-#" + thIdx + '%' + grid.name() + '%');

                IgniteCache<Integer, String> cache = grid.cache(DEFAULT_CACHE_NAME);

                Transaction tx = grid.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1);
                try {
                    if (rnd.nextBoolean())
                        tx.timeout(TX_TIMEOUT / 10 + rnd.nextInt(TX_TIMEOUT * 9 / 10));

                    int key = rnd.nextInt(grids.size());

                    IgniteFuture<Void> fut = cache.putAsync(key, Thread.currentThread().getName());

                    try {
                        fut.get(ROLLBACK_ON_TOPOLOGY_CHANGE_TIMEOUT / 2);
                    }
                    catch (IgniteFutureTimeoutException ignore) {
                    }
                    finally {
                        startTxLatch.countDown();

                        txStarted = true;
                    }

                    fut.get();

                    finishTxLatch.await();

                    // use a key from the different range to avoid deadlocks
                    cache.put(grids.size() + key, Thread.currentThread().getName());
                }
                catch (Exception e) {
                    Exception exp = null;

                    if (exp == null)
                        exp = X.cause(e, CacheStoppedException.class);

                    if (exp == null)
                        exp = X.cause(e, IgniteFutureCancelledException.class);

                    if (exp == null)
                        exp = X.cause(e, NodeStoppingException.class);

                    if (exp == null) {
                        exp = X.cause(e, ClusterTopologyCheckedException.class);

                        if (exp != null && !(exp.getMessage() != null && exp.getMessage().startsWith(
                            "Failed to acquire lock for keys (primary node left grid, retry transaction if possible)")))
                            exp = null;
                    }

                    if (exp == null) {
                        exp = X.cause(e, CacheInvalidStateException.class);

                        if (exp != null && !(exp.getMessage() != null && exp.getMessage().startsWith(
                            "Failed to perform cache operation (cluster is not activated)")))
                            exp = null;
                    }

                    if (exp == null) {
                        exp = X.cause(e, TransactionTimeoutException.class);

                        if (exp != null) {
                            String msg = exp.getMessage();
                            if (!(msg != null && (msg.startsWith("Cache transaction timed out: GridNearTxLocal") ||
                                msg.startsWith("Failed to acquire lock within provided timeout for transaction"))))
                            exp = null;
                        }
                    }

                    if (exp != null) {
                        if (grid.log().isInfoEnabled())
                            grid.log().info("Expected transaction error: " + exp);
                    }
                    else {
                        errors.add(e);

                        U.error(grid.log(), "Unexpected transaction error", e);
                    }
                }
                finally {
                    U.closeQuiet(tx);

                    closeTxLatch.countDown();
                }
            }
        }, txCount);

        startTxLatch.await();
    }

    /**  */
    private void checkTxErrors() throws Exception {
        finishTxLatch.countDown();

        closeTxLatch.await();

        for (Exception e : txErrors)
            U.error(log, "Unexpected transaction error", e);

        if (!txErrors.isEmpty())
            fail("Unexpected transaction error occurs. See log for details");
    }

    /**  */
    private void initMinorTopologyChangeOps() {
        shuffleOps = new ArrayList<>();

        shuffleOps.add(new Callable<Ignite>() {
            @Override public Ignite call() {
                Ignite grid = F.rand(G.allGrids());

                if (grid.log().isInfoEnabled())
                    grid.log().info(">>>> Deactivate cluster");

                grid.active(false);

                return grid;
            }
        });

        shuffleOps.add(new Callable<Ignite>() {
            @Override public Ignite call() {
                Ignite grid = F.rand(G.allGrids());

                if (grid.log().isInfoEnabled())
                    grid.log().info(">>>> Create cache");

                CacheConfiguration ccfg = defaultCacheConfiguration();

                ccfg.setName(TMP_CACHE);

                grid.createCache(ccfg);

                return grid;
            }
        });

        shuffleOps.add(new Callable<Ignite>() {
            @Override public Ignite call() {
                Ignite grid = F.rand(G.allGrids());

                if (grid.log().isInfoEnabled())
                    grid.log().info(">>>> Register binary type");

                String typeName = TxRollbackOnTopologyChangeTimeoutTest.class.getSimpleName() +
                    binaryTypeIdx.getAndIncrement();

                BinaryObjectBuilder builder = grid.binary().builder(typeName);

                builder.setField("gridName", grid.name());

                BinaryObject binObj = builder.build();

                IgniteCompute compute = grid.compute(grid.cluster());

                String res = compute.call(new GetBinaryObjectType(binObj));

                assertEquals(res, typeName);

                return grid;
            }
        });

        minorOps = new ReadOnlyCollectionView2X<>(shuffleOps, Collections.<Callable<Ignite>>singleton(new Callable<Ignite>() {
            @Override public Ignite call() {
                Ignite grid = F.rand(G.allGrids());

                if (grid.log().isInfoEnabled())
                    grid.log().info(">>>> Destroy cache");

                grid.cache(TMP_CACHE).destroy();

                return grid;
            }
        }));
    }

    /**  */
    private void doMinorTopologyChanges() throws Exception {
        Collections.shuffle(shuffleOps);

        for (Callable<Ignite> op : minorOps) {
            startTx();

            Ignite grid = op.call();

            checkTxErrors();

            if (!grid.active()) {
                if (grid.log().isInfoEnabled())
                    grid.log().info(">>>> Restore active state");

                grid.active(true);
            }
        }
    }

    /**  */
    private void startStopServers() throws Exception {
        if (log.isInfoEnabled())
            log.info(">>>> Start server node");

        startGrid(0);

        doMinorTopologyChanges();

        startStopClients();

        for (int i = 1; i < MAX_SRV_NODES; ++i) {
            startTx();

            if (log.isInfoEnabled())
                log.info(">>>> Start server node");

            startGrid(i);

            checkTxErrors();

            doMinorTopologyChanges();

            startStopClients();
        }
        for (int i = 0; i < MAX_SRV_NODES; ++i) {
            startTx();

            if (log.isInfoEnabled())
                log.info(">>>> Stop server node");

            stopGrid(i);

            checkTxErrors();

            if (i < MAX_CLNT_NODES - 1) {
                doMinorTopologyChanges();

                startStopClients();
            }
        }
    }

    /**  */
    private void startStopClients() throws Exception {
        for (int i = 0; i < MAX_CLNT_NODES; ++i) {
            if (log.isInfoEnabled())
                log.info(">>>> Start client node");

            startGrid(CLIENT_IDX + i);

            doMinorTopologyChanges();
        }
        for (int i = 0; i < MAX_CLNT_NODES; ++i) {
            startTx();

            if (log.isInfoEnabled())
                log.info(">>>> Stop client node");

            stopGrid(CLIENT_IDX + i);

            checkTxErrors();

            doMinorTopologyChanges();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", true);

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", true);

        binaryTypeIdx = new AtomicInteger();

        initMinorTopologyChangeOps();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        shuffleOps = null;

        minorOps = null;
    }

    /**  */
    private void testRollbackTxOnTopChange(int backups) throws Exception {
        assert backups <= MAX_BACKUPS : "Maximum backups is exceeded [cur=" + backups + ", max=" + MAX_BACKUPS + ']';

        this.backups = backups;

        try {
            startStopServers();
        }
        finally {
            stopAllGrids();
        }
    }

    /**  */
    public void testBackups0() throws Exception {
        testRollbackTxOnTopChange(0);
    }

    /**  */
    public void testBackups1() throws Exception {
        testRollbackTxOnTopChange(1);
    }

    /**  */
    public void testBackups2() throws Exception {
        testRollbackTxOnTopChange(2);
    }

    /**  */
    public void testBackups3() throws Exception {
        testRollbackTxOnTopChange(3);
    }

    /**  */
    private static class GetBinaryObjectType implements IgniteCallable<String> {

        /**  */
        private BinaryObject binObj;

        /**  */
        public GetBinaryObjectType(BinaryObject binObj) {
            this.binObj = binObj;
        }

        /** {@inheritDoc} */
        @Override public String call() throws Exception {
            return binObj.type().typeName();
        }
    }
}
