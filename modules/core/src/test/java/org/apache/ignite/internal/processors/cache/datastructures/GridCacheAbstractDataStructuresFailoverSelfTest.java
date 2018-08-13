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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Failover tests for cache data structures.
 */
public abstract class GridCacheAbstractDataStructuresFailoverSelfTest extends IgniteCollectionAbstractTest {
    /** */
    private static final long TEST_TIMEOUT = 3 * 60 * 1000;

    /** */
    private static final String NEW_IGNITE_INSTANCE_NAME = "newGrid";

    /** */
    private static final String STRUCTURE_NAME = "structure";

    /** */
    private static final String TRANSACTIONAL_CACHE_NAME = "tx_cache";

    /** */
    private static final String CLIENT_INSTANCE_NAME = "client";

    /** */
    private static final int TOP_CHANGE_CNT = 2;

    /** */
    private static final int TOP_CHANGE_THREAD_CNT = 2;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /**
     * @return Grids count to start.
     */
    @Override public int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(gridCount());

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();

        atomicCfg.setCacheMode(collectionCacheMode());
        atomicCfg.setBackups(collectionConfiguration().getBackups());

        cfg.setAtomicConfiguration(atomicCfg);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(TRANSACTIONAL_CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        if (client) {
            cfg.setClientMode(client);
            ((TcpDiscoverySpi)(cfg.getDiscoverySpi())).setForceServerMode(true);
        }

        return cfg;
    }

    /**
     * Starts client node.
     *
     * @return client node.
     * @throws Exception If failed.
     */
    protected IgniteEx startClient() throws Exception {
        return startGrid(getConfiguration(CLIENT_INSTANCE_NAME).setClientMode(true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockFailsWhenServersLeft() throws Exception {
        testReentrantLockFailsWhenServersLeft(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFairReentrantLockFailsWhenServersLeft() throws Exception {
        testReentrantLockFailsWhenServersLeft(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockFailsWhenServersLeft(final boolean fair) throws Exception {
        client = true;

        Ignite client = startGrid(gridCount());

        Ignite server = grid(0);

        // Initialize lock.
        IgniteLock srvLock = server.reentrantLock("lock", true, fair, true);

        IgniteSemaphore semaphore = server.semaphore("sync", 0, true, true);

        IgniteFuture fut = client.compute().applyAsync(new IgniteClosure<Ignite, Object>() {
            @Override public Object apply(Ignite ignite) {
                final IgniteLock l = ignite.reentrantLock("lock", true, fair, true);

                l.lock();

                assertTrue(l.isHeldByCurrentThread());

                l.unlock();

                assertFalse(l.isHeldByCurrentThread());

                // Signal the server to go down.
                ignite.semaphore("sync", 0, true, true).release();

                boolean isExceptionThrown = false;

                try {
                    // Wait for the server to go down.
                    Thread.sleep(1000);

                    l.lock();

                    fail("Exception must be thrown.");
                }
                catch (InterruptedException ignored) {
                    fail("Interrupted exception not expected here.");
                }
                catch (IgniteException ignored) {
                    isExceptionThrown = true;
                }
                finally {
                    assertTrue(isExceptionThrown);

                    assertFalse(l.isHeldByCurrentThread());
                }
                return null;
            }
        }, client);

        // Wait for the lock on client to be acquired then released.
        semaphore.acquire();

        for (int i = 0; i < gridCount(); i++)
            stopGrid(i);

        fut.get();

        client.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockConstantTopologyChangeFailoverSafe() throws Exception {
        doTestReentrantLock(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockConstantMultipleTopologyChangeFailoverSafe() throws Exception {
        doTestReentrantLock(multipleTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockConstantTopologyChangeNonFailoverSafe() throws Exception {
        doTestReentrantLock(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), false, false);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestReentrantLock(
        final ConstantTopologyChangeWorker topWorker,
        final boolean failoverSafe,
        final boolean fair
    ) throws Exception {
        IgniteEx ig = grid(0);

        try (IgniteLock lock = ig.reentrantLock(STRUCTURE_NAME, failoverSafe, fair, true)) {
            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Void>() {
                @Override public Void apply(Ignite ignite) {
                    final IgniteLock l = ignite.reentrantLock(STRUCTURE_NAME, failoverSafe, fair, false);

                    final AtomicBoolean done = new AtomicBoolean(false);

                    GridTestUtils.runAsync(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            try{
                                l.lock();
                            }
                            finally {
                                done.set(true);
                            }

                            return null;
                        }
                    }, "lock-thread");

                    // Wait until l.lock() has been called.
                    while(!l.hasQueuedThreads() && !done.get()){
                        // No-op.
                    }

                    return null;
                }
            });

            long endTime = System.currentTimeMillis() + getTestTimeout();

            while (!fut.isDone()) {
                try {
                    lock.lock();
                }
                catch (IgniteException e) {
                    // Exception may happen in non-failoversafe mode.
                    if (failoverSafe)
                        throw e;
                }
                finally {
                    // Broken lock cannot be used in non-failoversafe mode.
                    if(!lock.isBroken() || failoverSafe) {
                        assertTrue(lock.isHeldByCurrentThread());

                        lock.unlock();

                        assertFalse(lock.isHeldByCurrentThread());
                    }
                }

                if (System.currentTimeMillis() > endTime)
                    fail("Failed to wait for topology change threads.");
            }

            fut.get();

            for (Ignite g : G.allGrids()){
                IgniteLock l = g.reentrantLock(STRUCTURE_NAME, failoverSafe, fair, false);

                assertTrue(g.name(), !l.isHeldByCurrentThread() || lock.isBroken());
            }
        }
    }

    /**
     * @param topChangeThreads Number of topology change threads.
     *
     * @return Specific multiple topology change worker implementation.
     */
    private ConstantTopologyChangeWorker multipleTopologyChangeWorker(int topChangeThreads) {
        return collectionCacheMode() == CacheMode.PARTITIONED ?
            new PartitionedMultipleTopologyChangeWorker(topChangeThreads) :
            new MultipleTopologyChangeWorker(topChangeThreads);
    }

    /**
     *
     */
    private class ConstantTopologyChangeWorker {
        /** */
        protected final AtomicBoolean failed = new AtomicBoolean(false);

        /** */
        private final int topChangeThreads;

        /** Flag to enable circular topology change. */
        private boolean circular;

        /**
         * @param topChangeThreads Number of topology change threads.
         */
        public ConstantTopologyChangeWorker(int topChangeThreads) {
            this.topChangeThreads = topChangeThreads;
        }

        /**
         * @param topChangeThreads Number of topology change threads.
         * @param circular flag to enable circular topology change.
         */
        public ConstantTopologyChangeWorker(int topChangeThreads, boolean circular) {
            this.topChangeThreads = topChangeThreads;
            this.circular = circular;
        }

        /**
         * Starts changing cluster's topology.
         *
         * @param cb Callback to run after node start.
         * @return Future.
         */
        IgniteInternalFuture<?> startChangingTopology(final IgniteClosure<Ignite, ?> cb) {
            final AtomicInteger nodeIdx = new AtomicInteger(G.allGrids().size());

            return GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            if (failed.get())
                                return;

                            int idx = nodeIdx.getAndIncrement();

                            Thread.currentThread().setName("thread-" + getTestIgniteInstanceName(idx));

                            try {
                                log.info("Start node: " + getTestIgniteInstanceName(idx));

                                Ignite g = startGrid(idx);

                                cb.apply(g);
                            }
                            finally {
                                if(circular)
                                    stopGrid(G.allGrids().get(0).configuration().getIgniteInstanceName());
                                else
                                    stopGrid(idx);
                            }
                        }
                    }
                    catch (Exception e) {
                        if (failed.compareAndSet(false, true))
                            throw F.wrap(e);
                    }
                }
            }, topChangeThreads, "topology-change-thread");
        }
    }

    /**
     *
     */
    private class MultipleTopologyChangeWorker extends ConstantTopologyChangeWorker {
        /**
         * @param topChangeThreads Number of topology change threads.
         */
        public MultipleTopologyChangeWorker(int topChangeThreads) {
            super(topChangeThreads);
        }

        /**
         * Starts changing cluster's topology.
         *
         * @return Future.
         */
        @Override IgniteInternalFuture<?> startChangingTopology(final IgniteClosure<Ignite, ?> cb) {
            return GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            if (failed.get())
                                return;

                            Collection<String> names = new GridLeanSet<>(3);

                            try {
                                for (int j = 0; j < 3; j++) {
                                    if (failed.get())
                                        return;

                                    String name = UUID.randomUUID().toString();

                                    log.info("Start node: " + name);

                                    Ignite g = startGrid(name);

                                    names.add(name);

                                    cb.apply(g);
                                }
                            }
                            finally {
                                for (String name : names)
                                    stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        if (failed.compareAndSet(false, true))
                            throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");
        }
    }

    /**
     *
     */
    private class PartitionedMultipleTopologyChangeWorker extends ConstantTopologyChangeWorker {
        /** */
        private CyclicBarrier barrier;

        /**
         * @param topChangeThreads Number of topology change threads.
         */
        public PartitionedMultipleTopologyChangeWorker(int topChangeThreads) {
            super(topChangeThreads);
        }

        /**
         * Starts changing cluster's topology.
         *
         * @return Future.
         */
        @Override IgniteInternalFuture<?> startChangingTopology(final IgniteClosure<Ignite, ?> cb) {
            final Semaphore sem = new Semaphore(TOP_CHANGE_THREAD_CNT);

            final ConcurrentSkipListSet<String> startedNodes = new ConcurrentSkipListSet<>();

            barrier = new CyclicBarrier(TOP_CHANGE_THREAD_CNT, new Runnable() {
                @Override public void run() {
                    try {
                        assertEquals(TOP_CHANGE_THREAD_CNT * 3, startedNodes.size());

                        for (String name : startedNodes) {
                            stopGrid(name, false);

                            awaitPartitionMapExchange();
                        }

                        startedNodes.clear();

                        sem.release(TOP_CHANGE_THREAD_CNT);

                        barrier.reset();
                    }
                    catch (Exception e) {
                        if (failed.compareAndSet(false, true)) {
                            sem.release(TOP_CHANGE_THREAD_CNT);

                            barrier.reset();

                            throw F.wrap(e);
                        }
                    }
                }
            });

            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            sem.acquire();

                            if (failed.get())
                                return;

                            for (int j = 0; j < 3; j++) {
                                if (failed.get())
                                    return;

                                String name = UUID.randomUUID().toString();

                                startedNodes.add(name);

                                log.info("Start node: " + name);

                                Ignite g = startGrid(name);

                                cb.apply(g);
                            }

                            try {
                                barrier.await();
                            }
                            catch (BrokenBarrierException ignored) {
                                // No-op.
                            }
                        }
                    }
                    catch (Exception e) {
                        if (failed.compareAndSet(false, true)) {
                            sem.release(TOP_CHANGE_THREAD_CNT);

                            barrier.reset();

                            throw F.wrap(e);
                        }
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            return fut;
        }
    }
}
