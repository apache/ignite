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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests for "wait for backups on shutdown" flag.
 */
@WithSystemProperty(key = IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN, value = "true")
public class GridCacheDhtPreloadWaitForBackupsTest extends GridCommonAbstractTest {
    /** Key to store list of gracefully stopping nodes within metastore. */
    private static final String GRACEFUL_SHUTDOWN_METASTORE_KEY =
        DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "graceful.shutdown";

    /** Timeout to check stop node or not. */
    public static final int STOP_CHECK_TIMEOUT_LIMIT = 3_000;

    /** Hard timeout during that a node would to stopped. */
    public static final int STOP_TIMEOUT_LIMIT = 30_000;

    /** */
    private CacheMode cacheMode;

    /** */
    private CacheAtomicityMode atomicityMode;

    /** */
    private CacheWriteSynchronizationMode synchronizationMode;

    /** */
    private CacheRebalanceMode rebalanceMode;

    /** */
    private boolean clientNodes;

    /** */
    protected int backups;

    /** */
    public GridCacheDhtPreloadWaitForBackupsTest() {
        super(false);
    }

    /**
     * @return {@code True} if persistence must be enabled for test.
     */
    protected boolean persistenceEnabled() {
        return false;
    }

    /** */
    protected int cacheSize() {
        return 10_000;
    }

    /** */
    protected int iterations() {
        return 10;
    }

    /** */
    @Override protected void afterTest() throws Exception {
        IgnitionEx.stopAll(true, ShutdownPolicy.IMMEDIATE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeLeavesRebalanceCompletesAtomicReplicated() throws Exception {
        cacheMode = CacheMode.REPLICATED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        rebalanceMode = CacheRebalanceMode.SYNC;
        clientNodes = false;

        nodeLeavesRebalanceCompletes();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeLeavesRebalanceCompletesTransactionalPartitioned() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;
        synchronizationMode = CacheWriteSynchronizationMode.FULL_SYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = false;
        backups = 1;

        nodeLeavesRebalanceCompletes();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeLeavesRebalanceCompletesClientNode() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.FULL_ASYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = true;
        backups = 1;

        nodeLeavesRebalanceCompletes();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeForceShutdown() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = false;
        backups = 1;

        startGrids(2);

        if (persistenceEnabled())
            grid(0).cluster().active(true);

        for (int i = 0; i < cacheSize(); i++)
            grid(i % 2).cache("cache" + (1 + (i >> 3) % 3)).put(i, new byte[i]);

        grid(0).close();

        final CountDownLatch latch = new CountDownLatch(1);

        Thread stopper = new Thread(() -> {
            grid(1).close();
            latch.countDown();
        }, "Stopper");

        stopper.start();

        assertFalse(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, STOP_CHECK_TIMEOUT_LIMIT));

        IgnitionEx.stop(grid(1).configuration().getIgniteInstanceName(), true, ShutdownPolicy.IMMEDIATE, false);

        assertTrue(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, STOP_TIMEOUT_LIMIT));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedNodeLeavesImmediately() throws Exception {
        cacheMode = CacheMode.REPLICATED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = false;

        for (int n = 1; n <= 3; n++) {
            startGrids(1);

            if (persistenceEnabled())
                grid(0).cluster().active(true);

            for (int i = 0; i < cacheSize(); i++)
                grid(0).cache("cache" + (1 + (i >> 3) % 3)).put(i, new byte[i]);

            List<Thread> threads = new ArrayList<>();

            for (int i = 1; i <= n; i++) {
                Thread thread = new Thread(new GridStarter(i));

                thread.start();

                threads.add(thread);
            }

            grid(0).close();

            for (Thread thread : threads)
                thread.join();

            for (int i = 0; i < cacheSize(); i++) {
                byte[] val = (byte[])grid(1).cache("cache" + (1 + (i >> 3) % 3)).get(i);

                assertNotNull(Integer.toString(i), val);
                assertEquals(i, val.length);
            }

            IgnitionEx.stopAll(true, ShutdownPolicy.IMMEDIATE);

            if (persistenceEnabled())
                cleanPersistenceDir();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testShutdownWithoutBackups() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = false;
        backups = 3;

        startGrids(2);

        if (persistenceEnabled())
            grid(0).cluster().active(true);

        grid(1).cache("cache1").destroy();
        grid(1).cache("cache2").destroy();
        grid(1).cache("cache3").destroy();

        grid(1).createCache(new CacheConfiguration<>("no-backups").setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0));

        for (int i = 0; i < cacheSize(); i++)
            grid(i % 2).cache("no-backups").put(i, new byte[i]);

        for (int i = 1; i >= 0; i--) {
            final int n = i;

            Thread th = new Thread(() -> grid(n).close());

            th.start();

            th.join(60_000);

            assertFalse(th.isAlive());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThatItsNotPossibleToStopLastOwnerIfAnotherOwnerIsStopping() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.FULL_ASYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = true;
        backups = 1;

        startGrids(2);

        grid(0).cluster().active(true);

        grid(0).context().distributedMetastorage().write(
            DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "graceful.shutdown",
            new HashSet<>(Collections.singleton(grid(0).localNode().id())));

        final CountDownLatch latch = new CountDownLatch(1);

        Thread stopper = new Thread(() -> {
            grid(1).close();
            latch.countDown();
        }, "Stopper");

        stopper.start();

        assertFalse(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, STOP_CHECK_TIMEOUT_LIMIT));

        grid(0).context().distributedMetastorage().write(
            GRACEFUL_SHUTDOWN_METASTORE_KEY,
            new HashSet<>());

        assertTrue(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, STOP_TIMEOUT_LIMIT));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThatItsPossibleToStopNodeIfExludedNodeListWithinMetastoreIsntEmpty() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.FULL_ASYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = false;
        backups = 2;

        startGrids(3);

        grid(0).cluster().active(true);

        grid(0).context().distributedMetastorage().write(
            DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "graceful.shutdown",
            new HashSet<>(Collections.singleton(grid(0).localNode().id())));

        final CountDownLatch latch = new CountDownLatch(1);

        UUID node1Id = grid(1).localNode().id();

        Thread stopper = new Thread(() -> {
            grid(1).close();
            latch.countDown();
        }, "Stopper");

        stopper.start();

        assertTrue(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, STOP_TIMEOUT_LIMIT));

        HashSet<UUID> expMetastoreContent = new HashSet<>();
        expMetastoreContent.add(grid(0).localNode().id());
        expMetastoreContent.add(node1Id);

        assertEquals(
            expMetastoreContent,
            grid(0).context().distributedMetastorage().read(GRACEFUL_SHUTDOWN_METASTORE_KEY));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThatExcludedNodeListWithinMetastoreCleanedUpAfterUpdatingFullMap() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.FULL_ASYNC;
        rebalanceMode = CacheRebalanceMode.ASYNC;
        clientNodes = false;
        backups = 3;

        startGrids(4);

        grid(0).cluster().active(true);

        grid(0).context().distributedMetastorage().write(
            DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "graceful.shutdown",
            new HashSet<>(Collections.singleton(grid(0).localNode().id())));

        final CountDownLatch latch = new CountDownLatch(1);

        UUID node1Id = grid(1).localNode().id();

        Thread stopper = new Thread(() -> {
            grid(1).close();
            latch.countDown();
        }, "Stopper");

        stopper.start();

        assertTrue(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, STOP_TIMEOUT_LIMIT));

        HashSet<UUID> expMetastoreContent = new HashSet<>();
        expMetastoreContent.add(grid(0).localNode().id());
        expMetastoreContent.add(node1Id);

        assertEquals(
            expMetastoreContent,
            grid(0).context().distributedMetastorage().read(GRACEFUL_SHUTDOWN_METASTORE_KEY));

        UUID node2Id = grid(2).localNode().id();

        grid(2).close();

        expMetastoreContent.remove(node1Id);
        expMetastoreContent.add(node2Id);

        assertEquals(
            expMetastoreContent,
            grid(0).context().distributedMetastorage().read(GRACEFUL_SHUTDOWN_METASTORE_KEY));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThatItsNotPossibleToStopLastNodeInBaselineIfThereAreStilNonBaselineNodesInCluster()
        throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        backups = 1;

        startGrids(1);

        grid(0).cluster().baselineAutoAdjustTimeout(0);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        grid(0).cluster().active(true);

        for (int i = 0; i < cacheSize(); i++)
            grid(0).cache("cache" + (1 + (i >> 3) % 3)).put(i, i);

        startGrid(1);

        final CountDownLatch latch = new CountDownLatch(1);

        Thread stopper = new Thread(() -> {
            grid(0).close();
            latch.countDown();
        }, "Stopper");

        stopper.start();

        assertFalse(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, STOP_CHECK_TIMEOUT_LIMIT));

        grid(0).cluster().setBaselineTopology(Arrays.asList(grid(0).localNode(), grid(1).localNode()));

        assertTrue(GridTestUtils.waitForCondition(() -> latch.getCount() == 0, STOP_TIMEOUT_LIMIT));

        // Data shouldn't be lost.
        for (int i = 0; i < cacheSize(); i++)
            assertEquals(i, grid(1).cache("cache" + (1 + (i >> 3) % 3)).get(i));
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientNodeShouldStopImmediately() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        backups = 1;

        startGrid(0);

        startClientGrid(1);

        grid(0).cluster().active(true);

        grid(1).close();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeOnInactiveClusterShouldStopImmediately() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        backups = 1;

        startGrids(1);

        grid(0).cluster().active(false);

        grid(0).close();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollingRestartEmulation() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        backups = 1;

        int nodesCnt = 4;

        startGrids(nodesCnt);

        grid(0).cluster().active(true);

        for (int i = 0; i < cacheSize(); i++)
            grid(i % 2).cache("cache" + (1 + (i >> 3) % 3)).put(i, i);

        for (int rollingRestartCycles = 0; rollingRestartCycles < 2; rollingRestartCycles++) {
            for (int i = 0; i < nodesCnt; i++) {
                grid(i).close();

                startGrid(i);
            }
        }

        // Data shouldn't be lost.
        for (int i = 0; i < cacheSize(); i++)
            assertEquals(i, grid(i % 2).cache("cache" + (1 + (i >> 3) % 3)).get(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollingRestartEmulationWithOnlyHalfNodesInBaseline() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        backups = 1;

        int nodesCnt = 4;

        startGrids(nodesCnt / 2);

        ignite(0).cluster().baselineAutoAdjustEnabled(false);

        grid(0).cluster().active(true);

        for (int i = nodesCnt / 2; i < nodesCnt; i++)
            startGrid(i);

        for (int i = 0; i < cacheSize(); i++)
            grid(i % 2).cache("cache" + (1 + (i >> 3) % 3)).put(i, i);

        for (int rollingRestartCycles = 0; rollingRestartCycles < 2; rollingRestartCycles++) {
            for (int i = 0; i < nodesCnt; i++) {
                grid(i).close();

                startGrid(i);
            }
        }

        assertEquals(nodesCnt / 2, ignite(0).cluster().currentBaselineTopology().size());

        // Data shouldn't be lost.
        for (int i = 0; i < cacheSize(); i++)
            assertEquals(i, grid(i % 2).cache("cache" + (1 + (i >> 3) % 3)).get(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollingRestartEmulationReplicatedCache() throws Exception {
        cacheMode = CacheMode.REPLICATED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        rebalanceMode = CacheRebalanceMode.SYNC;

        int nodesCnt = 3;

        startGrids(nodesCnt);

        grid(0).cluster().active(true);

        for (int i = 0; i < cacheSize(); i++)
            grid(i % 2).cache("cache" + (1 + (i >> 3) % 3)).put(i, i);

        for (int rollingRestartCycles = 0; rollingRestartCycles < 2; rollingRestartCycles++) {
            for (int i = 0; i < nodesCnt; i++) {
                grid(i).close();

                startGrid(i);
            }
        }

        // Data shouldn't be lost.
        for (int i = 0; i < cacheSize(); i++)
            assertEquals(i, grid(i % 2).cache("cache" + (1 + (i >> 3) % 3)).get(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimultaneousSafeShutdown() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        backups = 1;

        startGrids(2);

        grid(0).cluster().active(true);

        for (int i = 0; i < cacheSize(); i++)
            grid(i % 2).cache("cache" + (1 + (i >> 3) % 3)).put(i, i);

        GridTestUtils.runAsync(() -> grid(0).close());

        GridTestUtils.runAsync(() -> grid(1).close());

        Thread.sleep(STOP_TIMEOUT_LIMIT);

        startGrid(2);

        assertTrue(waitForCondition(() -> grid(2).cluster().nodes().size() == 1, STOP_TIMEOUT_LIMIT));

        // Data shouldn't be lost.
        for (int i = 0; i < cacheSize(); i++)
            assertEquals(i, grid(2).cache("cache" + (1 + (i >> 3) % 3)).get(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimultaneousSafeShutdownWithReplicatedCache() throws Exception {
        cacheMode = CacheMode.REPLICATED;
        atomicityMode = CacheAtomicityMode.ATOMIC;
        synchronizationMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;
        rebalanceMode = CacheRebalanceMode.SYNC;

        startGrids(2);

        grid(0).cluster().active(true);

        for (int i = 0; i < cacheSize(); i++)
            grid(i % 2).cache("cache" + (1 + (i >> 3) % 3)).put(i, i);

        GridTestUtils.runAsync(() -> grid(0).close());

        GridTestUtils.runAsync(() -> grid(1).close());

        Thread.sleep(STOP_TIMEOUT_LIMIT);

        startGrid(2);

        assertTrue(waitForCondition(() -> grid(2).cluster().nodes().size() == 1, STOP_TIMEOUT_LIMIT));

        // Data shouldn't be lost.
        for (int i = 0; i < cacheSize(); i++)
            assertEquals(i, grid(2).cache("cache" + (1 + (i >> 3) % 3)).get(i));
    }

    /**
     * @throws Exception If failed.
     */
    private void nodeLeavesRebalanceCompletes() throws Exception {
        startGrids(4);

        if (persistenceEnabled())
            grid(0).cluster().active(true);

        for (int i = 0; i < cacheSize(); i++)
            grid(i % 4).cache("cache" + (1 + (i >> 3) % 3)).put(i, new byte[i]);

        int nextGrid = 4;

        Thread th0 = null;

        Thread th1 = null;

        for (int n = 0; n < iterations(); n++) {
            int startGrid = nextGrid;
            int stopPerm = (nextGrid + 1) % 5;
            int stopTmp = (nextGrid + 2) % 5;

            startGrid(startGrid);

            grid(stopTmp).close();

            (th0 = new Thread(() -> grid(stopPerm).close(), "Stop-" + stopPerm)).start();

            Thread.sleep(1000);

            (th1 = new Thread(new GridStarter(stopTmp), "Start-" + stopTmp)).start();

            nextGrid = stopPerm;

            th1.join();
            th0.join();
        }

        for (int i = 0; i < cacheSize(); i++) {
            byte[] val = (byte[])G.allGrids().get((i >> 2) % 4).cache("cache" + (1 + (i >> 3) % 3)).get(i);

            assertNotNull(Integer.toString(i), val);
            assertEquals(i, val.length);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration[] ccfgs = new CacheConfiguration[3];
        for (int i = 1; i <= 3; i++) {
            CacheConfiguration ccfg = new CacheConfiguration("cache" + i);

            ccfg.setCacheMode(cacheMode);
            ccfg.setAtomicityMode(atomicityMode);
            ccfg.setWriteSynchronizationMode(synchronizationMode);
            ccfg.setRebalanceMode(rebalanceMode);
            ccfg.setBackups(backups);
            ccfg.setAffinity(new RendezvousAffinityFunction().setPartitions(32));

            ccfgs[i - 1] = ccfg;
        }

        cfg.setCacheConfiguration(ccfgs);

        if (persistenceEnabled()) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration().setWalMode(LOG_ONLY)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(200L * 1024 * 1024)
                    .setPersistenceEnabled(true)));
        }

        if (clientNodes && (igniteInstanceName.endsWith("2") || igniteInstanceName.endsWith("3")))
            cfg.setClientMode(true);

        return cfg;
    }

    /** */
    private class GridStarter implements Runnable {
        /** */
        private final int n;

        /** */
        public GridStarter(int n) {
            this.n = n;
        }

        /** */
        @Override public void run() {
            try {
                Ignite node = startGrid(n);

                if (persistenceEnabled())
                    node.cluster().setBaselineTopology(node.cluster().forServers().nodes());
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
