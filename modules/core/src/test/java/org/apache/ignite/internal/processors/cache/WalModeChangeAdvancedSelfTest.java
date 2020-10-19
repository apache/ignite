/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Concurrent and advanced tests for WAL state change.
 */
@SuppressWarnings("unchecked")
public class WalModeChangeAdvancedSelfTest extends WalModeChangeCommonAbstractSelfTest {
    /**
     * Constructor.
     */
    public WalModeChangeAdvancedSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test cache cleanup on restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheCleanup() throws Exception {
        Ignite srv = startGrid(config(SRV_1, false, false));

        srv.cluster().active(true);

        IgniteCache cache1 = srv.getOrCreateCache(cacheConfig(CACHE_NAME, PARTITIONED, TRANSACTIONAL));
        IgniteCache cache2 = srv.getOrCreateCache(cacheConfig(CACHE_NAME_2, PARTITIONED, TRANSACTIONAL));

        assertForAllNodes(CACHE_NAME, true);
        assertForAllNodes(CACHE_NAME_2, true);

        for (int i = 0; i < 10; i++) {
            cache1.put(i, i);
            cache2.put(i, i);
        }

        srv.cluster().disableWal(CACHE_NAME);

        assertForAllNodes(CACHE_NAME, false);
        assertForAllNodes(CACHE_NAME_2, true);

        for (int i = 10; i < 20; i++) {
            cache1.put(i, i);
            cache2.put(i, i);
        }

        srv.cluster().disableWal(CACHE_NAME_2);

        assertForAllNodes(CACHE_NAME, false);
        assertForAllNodes(CACHE_NAME_2, false);

        for (int i = 20; i < 30; i++) {
            cache1.put(i, i);
            cache2.put(i, i);
        }

        assertEquals(cache1.size(), 30);
        assertEquals(cache2.size(), 30);

        srv.cluster().enableWal(CACHE_NAME);

        assertForAllNodes(CACHE_NAME, true);
        assertForAllNodes(CACHE_NAME_2, false);

        assertEquals(cache1.size(), 30);
        assertEquals(cache2.size(), 30);

        stopAllGrids(true);

        srv = startGrid(config(SRV_1, false, false));

        srv.cluster().active(true);

        cache1 = srv.cache(CACHE_NAME);
        cache2 = srv.cache(CACHE_NAME_2);

        assertForAllNodes(CACHE_NAME, true);
        assertForAllNodes(CACHE_NAME_2, false);

        assertEquals(30, cache1.size());
        assertEquals(0, cache2.size());
    }

    /**
     * Test simple node join.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testJoin() throws Exception {
        checkJoin(false);
    }

    /**
     * Test simple node join when operations is performed from coordinator.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testJoinCoordinator() throws Exception {
        checkJoin(true);
    }

    /**
     * Check node join behavior.
     *
     * @param crdFiltered {@code True} if first node is coordinator.
     * @throws Exception If failed.
     */
    private void checkJoin(boolean crdFiltered) throws Exception {
        // Start node and disable WAL.
        Ignite srv = startGrid(config(SRV_1, false, crdFiltered));

        srv.cluster().active(true);

        srv.getOrCreateCache(cacheConfig(PARTITIONED));
        assertForAllNodes(CACHE_NAME, true);

        if (!crdFiltered) {
            srv.cluster().disableWal(CACHE_NAME);
            assertForAllNodes(CACHE_NAME, false);
        }

        // Start other nodes.
        startGrid(config(SRV_2, false, false));

        if (crdFiltered)
            srv.cluster().disableWal(CACHE_NAME);

        assertForAllNodes(CACHE_NAME, false);

        startGrid(config(SRV_3, false, !crdFiltered));
        assertForAllNodes(CACHE_NAME, false);

        startGrid(config(CLI, true, false));
        assertForAllNodes(CACHE_NAME, false);

        // Stop nodes and restore WAL state on the first node.
        stopGrid(SRV_2, true);
        stopGrid(SRV_3, true);
        stopGrid(CLI, true);

        if (!crdFiltered) {
            srv.cluster().enableWal(CACHE_NAME);
            assertForAllNodes(CACHE_NAME, true);
        }

        // Start other nodes again.
        startGrid(config(SRV_2, false, false));

        if (crdFiltered)
            srv.cluster().enableWal(CACHE_NAME);

        assertForAllNodes(CACHE_NAME, true);

        startGrid(config(SRV_3, false, !crdFiltered));
        assertForAllNodes(CACHE_NAME, true);

        startGrid(config(CLI, true, false));
        assertForAllNodes(CACHE_NAME, true);
    }

    /**
     * Test server restart (non-coordinator).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerRestartNonCoordinator() throws Exception {
        checkNodeRestart(false);
    }

    /**
     * Test server restart (coordinator).
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7472")
    @Test
    public void testServerRestartCoordinator() throws Exception {
        checkNodeRestart(true);
    }

    /**
     * Test coordinator node migration.
     *
     * @param failCrd Whether to fail coordinator nodes.
     * @throws Exception If failed.
     */
    public void checkNodeRestart(boolean failCrd) throws Exception {
        startGrid(config(SRV_1, false, false));
        startGrid(config(SRV_2, false, false));

        Ignite cli = startGrid(config(CLI, true, false));

        cli.cluster().active(true);

        cli.getOrCreateCache(cacheConfig(PARTITIONED));

        final AtomicInteger restartCnt = new AtomicInteger();

        final int restarts = SF.applyLB(10, 3);

        Thread t = new Thread(new Runnable() {
            @Override public void run() {
                boolean firstOrSecond = true;

                while (restartCnt.get() < restarts) {
                    String victimName;

                    if (failCrd) {
                        victimName = firstOrSecond ? SRV_1 : SRV_2;

                        firstOrSecond = !firstOrSecond;
                    }
                    else
                        victimName = SRV_2;

                    try {
                        stopGrid(victimName);
                        startGrid(config(victimName, false, false));

                        Thread.sleep(500);
                    }
                    catch (Exception e) {
                        throw new RuntimeException();
                    }

                    restartCnt.incrementAndGet();

                    log.info(">>> Finished restart: " + restartCnt.get());
                }
            }
        });

        t.start();

        boolean state = true;

        while (restartCnt.get() < restarts && !Thread.currentThread().isInterrupted()) {
            try {
                if (state)
                    cli.cluster().disableWal(CACHE_NAME);
                else
                    cli.cluster().enableWal(CACHE_NAME);

                state = !state;
            }
            catch (IgniteException ignore) {
                // Possible disconnect, re-try.
            }
        }
    }

    /**
     * Test client re-connect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnect() throws Exception {
        final Ignite srv = startGrid(config(SRV_1, false, false));
        Ignite cli = startGrid(config(CLI, true, false));

        cli.cluster().active(true);

        cli.getOrCreateCache(cacheConfig(PARTITIONED));

        final AtomicBoolean done = new AtomicBoolean();

        // Start load.
        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            boolean state = false;

            while (!done.get()) {
                try {
                    if (state)
                        cli.cluster().enableWal(CACHE_NAME);
                    else
                        cli.cluster().disableWal(CACHE_NAME);
                }
                catch (IgniteException e) {
                    String msg = e.getMessage();

                    assert msg.startsWith("Client node disconnected") ||
                        msg.startsWith("Client node was disconnected") ||
                        msg.contains("client is disconnected") : e.getMessage();
                }
                finally {
                    state = !state;
                }
            }
        }, "wal-load-" + cli.name());

        // Now perform multiple client reconnects.
        try {
            for (int i = 1; i <= 10; i++) {
                Thread.sleep(ThreadLocalRandom.current().nextLong(200, 1000));

                IgniteClientReconnectAbstractTest.reconnectClientNode(log, cli, srv, new Runnable() {
                    @Override public void run() {
                        // No-op.
                    }
                });

                log.info(">>> Finished iteration: " + i);
            }
        } finally {
            done.set(true);
        }

        fut.get();
    }

    /**
     * Test client re-connect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheDestroy() throws Exception {
        final Ignite srv = startGrid(config(SRV_1, false, false));
        Ignite cli = startGrid(config(CLI, true, false));

        cli.cluster().active(true);

        srv.createCache(cacheConfig(PARTITIONED));

        final AtomicBoolean done = new AtomicBoolean();

        // Start load.
        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            boolean state = false;

            while (!done.get()) {
                try {
                    if (state)
                        cli.cluster().enableWal(CACHE_NAME);
                    else
                        cli.cluster().disableWal(CACHE_NAME);
                }
                catch (IgniteException e) {
                    String msg = e.getMessage();

                    assert msg.startsWith("Cache doesn't exist") ||
                        msg.startsWith("Failed to change WAL mode because some caches no longer exist") :
                        e.getMessage();
                }
                finally {
                    state = !state;
                }
            }
        }, "wal-load-" + cli.name());

        try {
            // Now perform multiple client reconnects.
            for (int i = 1; i <= 20; i++) {
                Thread.sleep(ThreadLocalRandom.current().nextLong(200, 1000));

                srv.destroyCache(CACHE_NAME);

                Thread.sleep(100);

                srv.createCache(cacheConfig(PARTITIONED));

                log.info(">>> Finished iteration: " + i);
            }
        }
        finally {
            done.set(true);
        }

        fut.get();
    }

    /**
     * Test that concurrent enable/disable events doesn't leave to hangs.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentOperations() throws Exception {
        final Ignite srv1 = startGrid(config(SRV_1, false, false));
        final Ignite srv2 = startGrid(config(SRV_2, false, false));
        final Ignite srv3 = startGrid(config(SRV_3, false, true));

        final Ignite cli = startGrid(config(CLI, true, false));

        final Ignite cacheCli = startGrid(config(CLI_2, true, false));

        cacheCli.cluster().active(true);

        final IgniteCache cache = cacheCli.getOrCreateCache(cacheConfig(PARTITIONED));

        for (int i = 1; i <= SF.applyLB(3, 2); i++) {
            // Start pushing requests.
            Collection<Ignite> walNodes = new ArrayList<>();

            walNodes.add(srv1);
            walNodes.add(srv2);
            walNodes.add(srv3);
            walNodes.add(cli);

            final AtomicBoolean done = new AtomicBoolean();

            final CountDownLatch latch = new CountDownLatch(walNodes.size() + 1);

            for (Ignite node : walNodes) {
                final Ignite node0 = node;

                Thread t = new Thread(new Runnable() {
                    @Override public void run() {
                        checkConcurrentOperations(done, node0);

                        latch.countDown();
                    }
                });

                t.setName("wal-load-" + node0.name());

                t.start();
            }

            // Do some cache loading in the mean time.
            Thread t = new Thread(new Runnable() {
                @Override public void run() {
                    int i = 0;

                    while (!done.get())
                        cache.put(i, i++);

                    latch.countDown();
                }
            });

            t.setName("cache-load");

            t.start();

            Thread.sleep(SF.applyLB(20_000, 2_000));

            done.set(true);

            log.info(">>> Stopping iteration: " + i);

            latch.await();

            log.info(">>> Iteration finished: " + i);
        }
    }

    /**
     * Check concurrent operations.
     *
     * @param done Done flag.
     * @param node Node.
     */
    private static void checkConcurrentOperations(AtomicBoolean done, Ignite node) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        boolean state = rnd.nextBoolean();

        while (!done.get()) {
            if (state)
                node.cluster().enableWal(CACHE_NAME);
            else
                node.cluster().disableWal(CACHE_NAME);

            state = !state;
        }

        try {
            Thread.sleep(rnd.nextLong(200, 1000));
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
