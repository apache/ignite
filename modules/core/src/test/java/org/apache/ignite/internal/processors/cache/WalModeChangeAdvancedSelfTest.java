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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteClientReconnectAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CORRUPTED_DATA_FILES_MNTC_TASK_NAME;

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
     * Verifies that node with consistent partitions (fully synchronized with disk on a previous checkpoint)
     * starts successfully even if WAL for that cache group is globally disabled.
     *
     * <p>
     *     Test scenario:
     * </p>
     * <ol>
     *     <li>
     *         Start a cluster from one server node, activate cluster.
     *     </li>
     *     <li>
     *         Create new cache. Disable WAL for the cache and put some data to it.
     *     </li>
     *     <li>
     *         Trigger checkpoint and wait for it finish. Restart node.
     *     </li>
     *     <li>
     *         Verify that node starts successfully and data is presented in the cache.
     *     </li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConsistentDataPreserved() throws Exception {
        Ignite srv = startGrid(config(SRV_1, false, false));

        srv.cluster().state(ACTIVE);

        IgniteCache cache1 = srv.getOrCreateCache(cacheConfig(CACHE_NAME, PARTITIONED, TRANSACTIONAL));

        srv.cluster().disableWal(CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache1.put(i, i);

        GridCacheDatabaseSharedManager dbMrg0 = (GridCacheDatabaseSharedManager)((IgniteEx)srv).context().cache().context().database();

        dbMrg0.forceCheckpoint("cp").futureFor(CheckpointState.FINISHED).get();

        stopGrid(SRV_1);

        srv = startGrid(config(SRV_1, false, false));

        assertForAllNodes(CACHE_NAME, false);

        cache1 = srv.cache(CACHE_NAME);

        for (int i = 0; i < 10; i++)
            assertNotNull(cache1.get(i));
    }

    /**
     * If user manually clears corrupted files when node was down, node detects this and not enters maintenance mode
     * (although still need another restart to get back to normal operations).
     *
     * <p>
     *     Test scenario:
     *     <ol>
     *         <li>
     *             Start server node, create cache, disable WAL for the cache, put some keys to it.
     *         </li>
     *         <li>
     *             Stop server node, remove checkpoint end markers from cp directory
     *             to make node think it has failed in the middle of checkpoint.
     *         </li>
     *         <li>
     *             Start the node, verify it fails to start because of corrupted PDS of the cache.
     *         </li>
     *         <li>
     *             Clean data directory of the cache, start node again, verify it doesn't report maintenance mode.
     *         </li>
     *         <li>
     *             Restart node and verify it is in normal operations mode.
     *         </li>
     *     </ol>
     * </p>
     *
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMaintenanceIsSkippedIfWasFixedManuallyOnDowntime() throws Exception {
        IgniteEx srv = startGrid(config(SRV_1, false, false));

        NodeFileTree ft = srv.context().pdsFolderResolver().fileTree();

        srv.cluster().state(ACTIVE);

        IgniteCache cache1 = srv.getOrCreateCache(cacheConfig(CACHE_NAME, PARTITIONED, TRANSACTIONAL));

        File cacheToClean = ft.defaultCacheStorage(srv.cachex(CACHE_NAME).configuration());

        srv.cluster().disableWal(CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache1.put(i, i);

        stopAllGrids(true);

        File[] cpMarkers = ft.checkpoint().listFiles();

        for (File cpMark : cpMarkers) {
            if (cpMark.getName().contains("-END"))
                cpMark.delete();
        }

        // Node should fail as its PDS may be corrupted because of disabled WAL
        GridTestUtils.assertThrows(null,
            () -> startGrid(config(SRV_1, false, false)),
            Exception.class,
            null);

        cleanCacheDir(cacheToClean);

        // Node should start successfully and enter maintenance mode. MaintenanceRecord will be cleaned
        // automatically because corrupted PDS was deleted during downtime
        srv = startGrid(config(SRV_1, false, false));
        assertTrue(srv.context().maintenanceRegistry().isMaintenanceMode());

        try {
            srv.context().maintenanceRegistry().actionsForMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);

            fail("Maintenance task is not completed yet for some reason.");
        }
        catch (Exception ignore) {
        }

        stopAllGrids(false);

        // After restart node works normal mode even without executing maintenance action to clear corrupted PDS
        srv = startGrid(config(SRV_1, false, false));
        assertFalse(srv.context().maintenanceRegistry().isMaintenanceMode());

        srv.cluster().state(ACTIVE);

        cache1 = srv.getOrCreateCache(CACHE_NAME);
        assertEquals(0, cache1.size());
    }

    /**
     * Test cache cleanup on restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheCleanup() throws Exception {
        IgniteEx srv = startGrid(config(SRV_1, false, false));

        srv.cluster().state(ACTIVE);

        IgniteCache cache1 = srv.getOrCreateCache(cacheConfig(CACHE_NAME, PARTITIONED, TRANSACTIONAL));
        IgniteCache cache2 = srv.getOrCreateCache(cacheConfig(CACHE_NAME_2, PARTITIONED, TRANSACTIONAL));

        File cacheToClean = srv.context().pdsFolderResolver().fileTree().defaultCacheStorage(srv.cachex(CACHE_NAME_2).configuration());

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

        cleanCacheDir(cacheToClean);

        srv = startGrid(config(SRV_1, false, false));

        srv.cluster().state(ACTIVE);

        cache1 = srv.cache(CACHE_NAME);
        cache2 = srv.cache(CACHE_NAME_2);

        assertForAllNodes(CACHE_NAME, true);
        assertForAllNodes(CACHE_NAME_2, false);

        assertEquals(30, cache1.size());
        assertEquals(0, cache2.size());
    }

    /** */
    private void cleanCacheDir(File cacheDir) {
        for (File f : cacheDir.listFiles()) {
            if (!NodeFileTree.cacheConfigFile(f))
                f.delete();
        }
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

        srv.cluster().state(ACTIVE);

        srv.getOrCreateCache(cacheConfig(PARTITIONED));
        assertForAllNodes(CACHE_NAME, true);

        if (!crdFiltered) {
            srv.cluster().disableWal(CACHE_NAME);
            assertForAllNodes(CACHE_NAME, false);
        }

        // Start other nodes.
        IgniteEx ig2 = startGrid(config(SRV_2, false, false));

        File ig2CacheDir = ig2.context().pdsFolderResolver().fileTree().defaultCacheStorage(ig2.cachex(CACHE_NAME).configuration());

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

        cleanCacheDir(ig2CacheDir);

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

        cli.cluster().state(ACTIVE);

        cli.getOrCreateCache(cacheConfig(PARTITIONED));

        final AtomicInteger restartCnt = new AtomicInteger();

        final int restarts = SF.applyLB(5, 3);

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
                        IgniteEx srv = grid(victimName);

                        File cacheDir =
                            srv.context().pdsFolderResolver().fileTree().defaultCacheStorage(srv.cachex(CACHE_NAME).configuration());

                        stopGrid(victimName);

                        cleanCacheDir(cacheDir);

                        startGrid(config(victimName, false, false));

                        Thread.sleep(200);
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

        cli.cluster().state(ACTIVE);

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
        }
        finally {
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

        cli.cluster().state(ACTIVE);

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
                        msg.startsWith("Failed to change WAL mode because cache group no longer exists") :
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

        cacheCli.cluster().state(ACTIVE);

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

    /**
     * Test disabling and enabling WAL for multiple caches simultaneously.
     *
     * <p>
     *     Test scenario:
     * </p>
     * <ol>
     *     <li>
     *         Start a cluster with multiple server nodes.
     *     </li>
     *     <li>
     *         Create multiple caches.
     *     </li>
     *     <li>
     *         Disable WAL for all caches in a single operation.
     *     </li>
     *     <li>
     *         Put some data to all caches.
     *     </li>
     *     <li>
     *         Enable WAL for a subset of caches in a single operation.
     *     </li>
     *     <li>
     *         Verify WAL state for all caches.
     *     </li>
     *     <li>
     *         Perform mixed operations with different cache subsets.
     *     </li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleCachesWalModeChange() throws Exception {
        // Start multiple server nodes
        Ignite srv1 = startGrid(config(SRV_1, false, false));
        startGrid(config(SRV_2, false, false));

        srv1.cluster().state(ACTIVE);

        // Create multiple caches with different configurations
        IgniteCache cache1 = srv1.getOrCreateCache(cacheConfig(CACHE_NAME, PARTITIONED, TRANSACTIONAL));
        IgniteCache cache2 = srv1.getOrCreateCache(cacheConfig(CACHE_NAME_2, PARTITIONED, TRANSACTIONAL));
        IgniteCache cache3 = srv1.getOrCreateCache(cacheConfig(CACHE_NAME_3, PARTITIONED, TRANSACTIONAL));

        // Initially, WAL should be enabled for all caches
        assertForAllNodes(CACHE_NAME, true);
        assertForAllNodes(CACHE_NAME_2, true);
        assertForAllNodes(CACHE_NAME_3, true);

        // Test 1: Disable WAL for all caches in one operation
        Collection<String> allCaches = Arrays.asList(CACHE_NAME, CACHE_NAME_2, CACHE_NAME_3);
        boolean changed = srv1.cluster().disableWal(allCaches);

        assertTrue("WAL state should be changed for at least one cache", changed);

        // Verify WAL is disabled for all caches on all nodes
        assertForAllNodes(CACHE_NAME, false);
        assertForAllNodes(CACHE_NAME_2, false);
        assertForAllNodes(CACHE_NAME_3, false);

        // Put data to all caches with WAL disabled
        for (int i = 0; i < 100; i++) {
            cache1.put(i, "value1-" + i);
            cache2.put(i, "value2-" + i);
            cache3.put(i, "value3-" + i);
        }

        // Test 2: Enable WAL for a subset of caches
        Collection<String> subsetCaches = Arrays.asList(CACHE_NAME, CACHE_NAME_3);
        changed = srv1.cluster().enableWal(subsetCaches);

        assertTrue("WAL state should be changed for at least one cache", changed);

        // Verify mixed WAL states
        assertForAllNodes(CACHE_NAME, true);   // Enabled
        assertForAllNodes(CACHE_NAME_2, false); // Still disabled
        assertForAllNodes(CACHE_NAME_3, true);  // Enabled

        // Put more data with mixed WAL states
        for (int i = 100; i < 200; i++) {
            cache1.put(i, "value1-" + i);
            cache2.put(i, "value2-" + i);
            cache3.put(i, "value3-" + i);
        }

        // Test 3: Disable WAL for cache that already has WAL disabled (no-op)
        Collection<String> singleCache = Collections.singletonList(CACHE_NAME_2);
        changed = srv1.cluster().disableWal(singleCache);

        assertFalse("WAL state should not change (already disabled)", changed);
        assertForAllNodes(CACHE_NAME_2, false); // Still disabled

        // Test 4: Enable WAL for all caches
        changed = srv1.cluster().enableWal(allCaches);

        // cache1 and cache3 already have WAL enabled, only cache2 should change
        assertTrue("WAL state should be changed for cache2", changed);

        // Verify all caches have WAL enabled
        assertForAllNodes(CACHE_NAME, true);
        assertForAllNodes(CACHE_NAME_2, true);
        assertForAllNodes(CACHE_NAME_3, true);

        // Test 5: Mixed operations with overlapping cache sets
        // Disable WAL for cache1 and cache2
        Collection<String> cache12 = Arrays.asList(CACHE_NAME, CACHE_NAME_2);
        changed = srv1.cluster().disableWal(cache12);
        assertTrue("WAL state should be changed for both caches", changed);

        // Enable WAL for cache2 and cache3 (cache2 changes, cache3 already enabled)
        Collection<String> cache23 = Arrays.asList(CACHE_NAME_2, CACHE_NAME_3);
        changed = srv1.cluster().enableWal(cache23);
        assertTrue("WAL state should be changed for cache2", changed);

        // Final verification
        assertForAllNodes(CACHE_NAME, false);  // Disabled
        assertForAllNodes(CACHE_NAME_2, true); // Enabled
        assertForAllNodes(CACHE_NAME_3, true); // Enabled

        // Verify data is still accessible
        assertEquals(200, cache1.size());
        assertEquals(200, cache2.size());
        assertEquals(200, cache3.size());

        for (int i = 0; i < 100; i++) {
            assertEquals("value1-" + i, cache1.get(i));
            assertEquals("value2-" + i, cache2.get(i));
            assertEquals("value3-" + i, cache3.get(i));
        }
    }
}

