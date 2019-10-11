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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_REBALANCE_BATCH_SIZE;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test node restart.
 */
public abstract class GridCacheAbstractNodeRestartSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    protected static final String CACHE_NAME = "TEST_CACHE";

    /** */
    private static final long TEST_TIMEOUT = 5 * 60 * 1000;

    /** Default backups. */
    private static final int DFLT_BACKUPS = 1;

    /** Partitions. */
    private static final int DFLT_PARTITIONS = 521;

    /** Preload batch size. */
    private static final int DFLT_BATCH_SIZE = DFLT_REBALANCE_BATCH_SIZE;

    /** Number of key backups. Each test method can set this value as required. */
    protected int backups = DFLT_BACKUPS;

    /** */
    private static final int DFLT_NODE_CNT = 4;

    /** */
    private static final int DFLT_KEY_CNT = 100;

    /** */
    private static final int DFLT_RETRIES = 10;

    /** */
    private static final int LOG_FREQ = 1000;

    /** */
    private static final Random RAND = new Random();

    /** */
    private static volatile int idx = -1;

    /** Preload mode. */
    protected CacheRebalanceMode rebalancMode = ASYNC;

    /** */
    protected boolean evict = false;

    /** */
    protected int rebalancBatchSize = DFLT_BATCH_SIZE;

    /** Number of partitions. */
    protected int partitions = DFLT_PARTITIONS;

    /** Node count. */
    protected int nodeCnt = DFLT_NODE_CNT;

    /** Key count. */
    protected int keyCnt = DFLT_KEY_CNT;

    /** Retries. */
    private int retries = DFLT_RETRIES;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)c.getCommunicationSpi()).setSharedMemoryPort(-1);

        // Discovery.
        TcpDiscoverySpi disco = (TcpDiscoverySpi)c.getDiscoverySpi();

        disco.setSocketTimeout(30_000);
        disco.setAckTimeout(30_000);
        disco.setNetworkTimeout(30_000);

        CacheConfiguration ccfg = cacheConfiguration();

        if (evict) {
            LruEvictionPolicy plc = new LruEvictionPolicy();

            plc.setMaxSize(100);

            ccfg.setEvictionPolicy(plc);
            ccfg.setOnheapCacheEnabled(true);
        }

        c.setCacheConfiguration(ccfg);

        return c;
    }

    /**
     * @return Cache configuration.
     */
    protected abstract CacheConfiguration cacheConfiguration();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = DFLT_BACKUPS;
        partitions = DFLT_PARTITIONS;
        rebalancMode = ASYNC;
        evict = false;
        rebalancBatchSize = DFLT_BATCH_SIZE;
        nodeCnt = DFLT_NODE_CNT;
        keyCnt = DFLT_KEY_CNT;
        retries = DFLT_RETRIES;
        idx = -1;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /**
     * @throws Exception If failed.
     */
    private void startGrids() throws Exception {
        for (int i = 0; i < nodeCnt; i++) {
            startGrid(i);

            if (idx < 0)
                idx = i;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestart() throws Exception {
        rebalancMode = SYNC;
        partitions = 3;
        nodeCnt = 2;
        keyCnt = 10;
        retries = 3;

        info("*** STARTING TEST ***");

        startGrids();

        try {
            IgniteCache<Integer, String> c = grid(idx).cache(CACHE_NAME);

            for (int j = 0; j < retries; j++) {
                for (int i = 0; i < keyCnt; i++)
                    c.put(i, Integer.toString(i));

                info("Stored items.");

                checkGet(c, j);

                info("Stopping node: " + idx);

                stopGrid(idx);

                info("Starting node: " + idx);

                Ignite ignite = startGrid(idx);

                c = ignite.cache(CACHE_NAME);

                checkGet(c, j);
            }
        }
        finally {
            stopAllGrids(true);
        }
    }

    /**
     * @param c Cache.
     * @param attempt Attempt.
     * @throws Exception If failed.
     */
    private void checkGet(IgniteCache<Integer, String> c, int attempt) throws Exception {
        for (int i = 0; i < keyCnt; i++) {
            String v = c.get(i);

            if (v == null) {
                printFailureDetails(c, i, attempt);

                fail("Value is null [key=" + i + ", attempt=" + attempt + "]");
            }

            if (!Integer.toString(i).equals(v)) {
                printFailureDetails(c, i, attempt);

                fail("Wrong value for key [key=" +
                    i + ", actual value=" + v + ", expected value=" + Integer.toString(i) + "]");
            }
        }

        info("Read items.");
    }

    /**
     * @return Transaction concurrency to use in tests.
     */
    protected TransactionConcurrency txConcurrency() {
        return PESSIMISTIC;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithPutTwoNodesNoBackups() throws Throwable {
        backups = 0;
        nodeCnt = 2;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 3_000);

        checkRestartWithPut(duration, 1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithTxTwoNodesNoBackups() throws Throwable {
        backups = 0;
        nodeCnt = 2;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 3_000);

        checkRestartWithTx(duration, 1, 1, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithPutTwoNodesOneBackup() throws Throwable {
        backups = 1;
        nodeCnt = 2;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 3_000);

        checkRestartWithPut(duration, 1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithTxTwoNodesOneBackup() throws Throwable {
        backups = 1;
        nodeCnt = 2;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 3_000);

        checkRestartWithTx(duration, 1, 1, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithPutFourNodesNoBackups() throws Throwable {
        backups = 0;
        nodeCnt = 4;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 5_000);

        checkRestartWithPut(duration, 2, 2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithTxFourNodesNoBackups() throws Throwable {
        backups = 0;
        nodeCnt = 4;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 5_000);

        checkRestartWithTx(duration, 2, 2, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithPutFourNodesOneBackups() throws Throwable {
        backups = 1;
        nodeCnt = 4;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 5_000);

        checkRestartWithPut(duration, 2, 2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithPutFourNodesOneBackupsOffheapEvict() throws Throwable {
        backups = 1;
        nodeCnt = 4;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = true;

        long duration = SF.applyLB(30_000, 5_000);

        checkRestartWithPut(duration, 2, 2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithTxFourNodesOneBackups() throws Throwable {
        backups = 1;
        nodeCnt = 4;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 5_000);

        checkRestartWithTx(duration, 2, 2, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithTxFourNodesOneBackupsOffheapEvict() throws Throwable {
        backups = 1;
        nodeCnt = 4;
        keyCnt = 100_000;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = true;

        long duration = SF.applyLB(30_000, 3_000);

        checkRestartWithTx(duration, 2, 2, 100);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithPutSixNodesTwoBackups() throws Throwable {
        backups = 2;
        nodeCnt = 6;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 6_000);

        checkRestartWithPut(duration, 3, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithTxSixNodesTwoBackups() throws Throwable {
        backups = 2;
        nodeCnt = 6;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 6_000);

        checkRestartWithTx(duration, 3, 3, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithPutEightNodesTwoBackups() throws Throwable {
        backups = 2;
        nodeCnt = 8;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 6_000);

        checkRestartWithPut(duration, 4, 4);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithTxEightNodesTwoBackups() throws Throwable {
        backups = 2;
        nodeCnt = 8;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 6_000);

        checkRestartWithTx(duration, 4, 4, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithPutTenNodesTwoBackups() throws Throwable {
        backups = 2;
        nodeCnt = 10;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 6_000);

        checkRestartWithPut(duration, 5, 5);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithTxTenNodesTwoBackups() throws Throwable {
        backups = 2;
        nodeCnt = 10;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 6_000);

        checkRestartWithTx(duration, 5, 5, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithTxPutAllTenNodesTwoBackups() throws Throwable {
        backups = 2;
        nodeCnt = 10;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 6_000);

        checkRestartWithTxPutAll(duration, 5, 5);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithTxPutAllFourNodesTwoBackups() throws Throwable {
        backups = 2;
        nodeCnt = 4;
        keyCnt = 10;
        partitions = 29;
        rebalancMode = ASYNC;
        evict = false;

        long duration = SF.applyLB(30_000, 6_000);

        checkRestartWithTxPutAll(duration, 2, 2);
    }

    /**
     * @param duration Test duration.
     * @param putThreads Put threads count.
     * @param restartThreads Restart threads count.
     * @throws Exception If failed.
     */
    private void checkRestartWithPut(long duration, int putThreads, int restartThreads) throws Throwable {
        final long endTime = System.currentTimeMillis() + duration;

        final AtomicReference<Throwable> err = new AtomicReference<>();

        startGrids();

        Collection<Thread> threads = new LinkedList<>();

        try {
            final AtomicInteger putCntr = new AtomicInteger();

            final CyclicBarrier barrier = new CyclicBarrier(putThreads + restartThreads);

            for (int i = 0; i < putThreads; i++) {
                final int gridIdx = i;

                Thread t = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            barrier.await();

                            info("Starting put thread: " + gridIdx);

                            Thread.currentThread().setName("put-worker-" + grid(gridIdx).name());

                            IgniteCache<Integer, String> cache = grid(gridIdx).cache(CACHE_NAME);

                            while (System.currentTimeMillis() < endTime && err.get() == null) {
                                int key = RAND.nextInt(keyCnt);

                                try {
                                    cache.put(key, Integer.toString(key));
                                }
                                catch (IgniteException | CacheException ignored) {
                                    // It is ok if primary node leaves grid.
                                }

                                cache.get(key);

                                int c = putCntr.incrementAndGet();

                                if (c % LOG_FREQ == 0)
                                    info(">>> Put iteration [cnt=" + c + ", key=" + key + ']');
                            }
                        }
                        catch (Exception e) {
                            err.compareAndSet(null, e);

                            error("Unexpected exception in put-worker.", e);
                        }
                    }
                }, "put-worker-" + i);

                t.start();

                threads.add(t);
            }

            for (int i = 0; i < restartThreads; i++) {
                final int gridIdx = i + putThreads;

                Thread t = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            barrier.await();

                            info("Starting restart thread: " + gridIdx);

                            int cnt = 0;

                            while (System.currentTimeMillis() < endTime && err.get() == null) {
                                log.info(">>>>>>> Stopping grid " + gridIdx);

                                stopGrid(gridIdx);

                                log.info(">>>>>>> Starting grid " + gridIdx);

                                startGrid(gridIdx);

                                int c = ++cnt;

                                if (c % LOG_FREQ == 0)
                                    info(">>> Restart iteration: " + c);
                            }
                        }
                        catch (Exception e) {
                            err.compareAndSet(null, e);

                            error("Unexpected exception in restart-worker.", e);
                        }
                    }
                }, "restart-worker-" + i);

                t.start();

                threads.add(t);
            }

            for (Thread t : threads)
                t.join(2 * duration);

            for (Thread t : threads) {
                if (t.isAlive())
                    t.interrupt();
            }

            if (err.get() != null)
                throw err.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param duration Test duration.
     * @param putThreads Put threads count.
     * @param restartThreads Restart threads count.
     * @param txKeys Keys per transaction.
     * @throws Exception If failed.
     */
    private void checkRestartWithTx(long duration,
        int putThreads,
        int restartThreads,
        final int txKeys) throws Throwable {
        if (atomicityMode() == ATOMIC)
            return;

        final long endTime = System.currentTimeMillis() + duration;

        final AtomicReference<Throwable> err = new AtomicReference<>();

        startGrids();

        Collection<Thread> threads = new LinkedList<>();

        try {
            final AtomicInteger txCntr = new AtomicInteger();

            final CyclicBarrier barrier = new CyclicBarrier(putThreads + restartThreads);

            for (int i = 0; i < putThreads; i++) {
                final int gridIdx = i;

                Thread t = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            barrier.await();

                            info("Starting put thread: " + gridIdx);

                            Ignite ignite = grid(gridIdx);

                            Thread.currentThread().setName("put-worker-" + ignite.name());

                            UUID locNodeId = ignite.cluster().localNode().id();

                            IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME).withAllowAtomicOpsInTx();

                            List<Integer> keys = new ArrayList<>(txKeys);

                            while (System.currentTimeMillis() < endTime && err.get() == null) {
                                keys.clear();

                                for (int i = 0; i < txKeys; i++)
                                    keys.add(RAND.nextInt(keyCnt));

                                // Ensure lock order.
                                Collections.sort(keys);

                                int c = 0;

                                try {
                                    IgniteTransactions txs = ignite.transactions();

                                    try (Transaction tx = txs.txStart(txConcurrency(), REPEATABLE_READ)) {
                                        c = txCntr.incrementAndGet();

                                        if (c % LOG_FREQ == 0) {
                                            info(">>> Tx iteration started [cnt=" + c +
                                                ", keys=" + keys +
                                                ", locNodeId=" + locNodeId + ']');
                                        }

                                        for (int key : keys) {
                                            int op = cacheOp();

                                            if (op == 1)
                                                cache.put(key, Integer.toString(key));
                                            else if (op == 2)
                                                cache.remove(key);
                                            else
                                                cache.get(key);
                                        }

                                        tx.commit();
                                    }
                                }
                                catch (IgniteException | CacheException ignored) {
                                    // It is ok if primary node leaves grid.
                                }

                                if (c % LOG_FREQ == 0) {
                                    info(">>> Tx iteration finished [cnt=" + c +
                                        ", cacheSize=" + cache.localSize() +
                                        ", keys=" + keys +
                                        ", locNodeId=" + locNodeId + ']');
                                }
                            }

                            info(">>> " + Thread.currentThread().getName() + " finished.");
                        }
                        catch (Exception e) {
                            err.compareAndSet(null, e);

                            error("Unexpected exception in put-worker.", e);
                        }
                    }
                }, "put-worker-" + i);

                t.start();

                threads.add(t);
            }

            for (int i = 0; i < restartThreads; i++) {
                final int gridIdx = i + putThreads;

                Thread t = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            barrier.await();

                            info("Starting restart thread: " + gridIdx);

                            int cnt = 0;

                            while (System.currentTimeMillis() < endTime && err.get() == null) {
                                stopGrid(getTestIgniteInstanceName(gridIdx), false, false);
                                startGrid(gridIdx);

                                int c = ++cnt;

                                if (c % LOG_FREQ == 0)
                                    info(">>> Restart iteration: " + c);
                            }

                            info(">>> " + Thread.currentThread().getName() + " finished.");
                        }
                        catch (Exception e) {
                            err.compareAndSet(null, e);

                            error("Unexpected exception in restart-worker.", e);
                        }
                    }
                }, "restart-worker-" + i);

                t.start();

                threads.add(t);
            }

            for (Thread t : threads)
                t.join();

            if (err.get() != null)
                throw err.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param duration Test duration.
     * @param putThreads Put threads count.
     * @param restartThreads Restart threads count.
     * @throws Exception If failed.
     */
    private void checkRestartWithTxPutAll(long duration, int putThreads, int restartThreads) throws Throwable {
        if (atomicityMode() == ATOMIC)
            return;

        final long endTime = System.currentTimeMillis() + duration;

        final AtomicReference<Throwable> err = new AtomicReference<>();

        startGrids();

        Collection<Thread> threads = new LinkedList<>();

        try {
            final AtomicInteger txCntr = new AtomicInteger();

            final CyclicBarrier barrier = new CyclicBarrier(putThreads + restartThreads);

            final int txKeys = 3;

            for (int i = 0; i < putThreads; i++) {
                final int gridIdx = i;

                Thread t = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            barrier.await();

                            info("Starting put thread: " + gridIdx);

                            Ignite ignite = grid(gridIdx);

                            Thread.currentThread().setName("put-worker-" + ignite.name());

                            UUID locNodeId = ignite.cluster().localNode().id();

                            IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME).withAllowAtomicOpsInTx();

                            List<Integer> keys = new ArrayList<>(txKeys);

                            while (System.currentTimeMillis() < endTime && err.get() == null) {
                                keys.clear();

                                for (int i = 0; i < txKeys; i++)
                                    keys.add(RAND.nextInt(keyCnt));

                                // Ensure lock order.
                                Collections.sort(keys);

                                int c = 0;

                                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                    c = txCntr.incrementAndGet();

                                    if (c % LOG_FREQ == 0)
                                        info(">>> Tx iteration started [cnt=" + c + ", keys=" + keys + ", " +
                                            "locNodeId=" + locNodeId + ']');

                                    Map<Integer, String> batch = new LinkedHashMap<>();

                                    for (int key : keys)
                                        batch.put(key, String.valueOf(key));

                                    cache.putAll(batch);

                                    tx.commit();
                                }
                                catch (IgniteException | CacheException ignored) {
                                    // It is ok if primary node leaves grid.
                                }

                                if (c % LOG_FREQ == 0) {
                                    info(">>> Tx iteration finished [cnt=" + c +
                                        ", keys=" + keys + ", " +
                                        "locNodeId=" + locNodeId + ']');
                                }
                            }
                        }
                        catch (Exception e) {
                            err.compareAndSet(null, e);

                            error("Unexpected exception in put-worker.", e);
                        }
                    }
                }, "put-worker-" + i);

                t.start();

                threads.add(t);
            }

            for (int i = 0; i < restartThreads; i++) {
                final int gridIdx = i + putThreads;

                Thread t = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            barrier.await();

                            info("Starting restart thread: " + gridIdx);

                            int cnt = 0;

                            while (System.currentTimeMillis() < endTime && err.get() == null) {
                                stopGrid(gridIdx);
                                startGrid(gridIdx);

                                int c = ++cnt;

                                if (c % LOG_FREQ == 0)
                                    info(">>> Restart iteration: " + c);
                            }
                        }
                        catch (Exception e) {
                            err.compareAndSet(null, e);

                            error("Unexpected exception in restart-worker.", e);
                        }
                    }
                }, "restart-worker-" + i);

                t.start();

                threads.add(t);
            }

            for (Thread t : threads)
                t.join();

            if (err.get() != null)
                throw err.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @return Cache operation.
     */
    private int cacheOp() {
        return RAND.nextInt(3) + 1;
    }

    /**
     * @param c Cache projection.
     * @param key Key.
     * @param attempt Attempt.
     */
    private void printFailureDetails(IgniteCache<Integer, String> c, int key, int attempt) {
        Ignite ignite = c.unwrap(Ignite.class);

        error("*** Failure details ***");
        error("Key: " + key);
        error("Partition: " + ignite.affinity(c.getName()).partition(key));
        error("Attempt: " + attempt);
        error("Node: " + ignite.cluster().localNode().id());
    }
}
