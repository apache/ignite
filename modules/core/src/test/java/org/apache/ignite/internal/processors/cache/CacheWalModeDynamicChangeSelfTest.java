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

import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Checks cluster wide WAL mode change operation.
 */
@SuppressWarnings("unchecked")
public class CacheWalModeDynamicChangeSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Cache 1. */
    private static final String CACHE1 = "cache1";

    /** Cache 2. */
    private static final String CACHE2 = "cache2";

    /** Cache 3. */
    private static final String CACHE3 = "cache3";

    /** Region. */
    private static final String REGION = "region";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(3).equals(igniteInstanceName))
            iCfg.setClientMode(true);

        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setIpFinder(ipFinder);

        DataRegionConfiguration reg = new DataRegionConfiguration().setName(REGION).setPersistenceEnabled(true);
        DataRegionConfiguration dfltReg = new DataRegionConfiguration().setName("dflt").setPersistenceEnabled(true);

        iCfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDataRegionConfigurations(reg)
                .setDefaultDataRegionConfiguration(dfltReg)
        );

        CacheConfiguration cache1 = new CacheConfiguration(CACHE1).setDataRegionName(REGION);
        CacheConfiguration cache2 = new CacheConfiguration(CACHE2).setGroupName("group").setDataRegionName(REGION);
        CacheConfiguration cache3 = new CacheConfiguration(CACHE3).setGroupName("group").setDataRegionName(REGION);

        iCfg.setCacheConfiguration(cache1, cache2, cache3);

        return iCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Removing persistence data
        U.delete(Paths.get(U.defaultWorkDirectory()).toFile());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     *
     */
    public void testRestoreAfterEnabling() throws Exception {
        final Ignite ignite1 = startGrid(1);
        final Ignite ignite2 = startGrid(2);
        final Ignite client3 = startGrid(3);

        client3.active(true);

        GridCacheSharedContext ctx1 = ((IgniteKernal)ignite1).context().cache().context();
        GridCacheSharedContext ctxClient = ((IgniteKernal)client3).context().cache().context();

        final int g1 = ctx1.cache().cacheDescriptor(CACHE1).groupId();
        final int g2 = ctx1.cache().cacheDescriptor(CACHE2).groupId();
        final int g3 = ctx1.cache().cacheDescriptor(CACHE3).groupId();

        assertFalse(ctx1.cache().cacheGroup(g1).walDisabled());
        assertFalse(ctx1.cache().cacheGroup(g2).walDisabled());
        assertFalse(ctx1.cache().cacheGroup(g3).walDisabled());

        assertTrue(ignite1.cluster().isWalEnabled(CACHE1));
        assertTrue(ignite2.cluster().isWalEnabled(CACHE1));
        assertTrue(client3.cluster().isWalEnabled(CACHE1));

        client3.cluster().disableWal(CACHE1);

        assertFalse(ignite1.cluster().isWalEnabled(CACHE1));
        assertFalse(ignite2.cluster().isWalEnabled(CACHE1));
        assertFalse(client3.cluster().isWalEnabled(CACHE1));

        // Checking newcomer know that wal disabled
        Ignite ignite4 = startGrid(4);

        GridCacheSharedContext ctx4 = ((IgniteKernal)ignite4).context().cache().context();

        assertTrue(ctx4.cache().cacheGroup(g1).walDisabled());
        assertFalse(ctx4.cache().cacheGroup(g2).walDisabled());
        assertFalse(ctx4.cache().cacheGroup(g3).walDisabled());

        // Checking old node
        assertTrue(ctx1.cache().cacheGroup(g1).walDisabled());
        assertFalse(ctx1.cache().cacheGroup(g2).walDisabled());
        assertFalse(ctx1.cache().cacheGroup(g3).walDisabled());

        // Streaming
        final IgniteCache cache1 = client3.getOrCreateCache(CACHE1);

        int size = 100_000;

        for (int i = 0; i < size; i++)
            cache1.put(i, i);

        assertEquals(size, cache1.size());

        final CountDownLatch opsRestricted = new CountDownLatch(2);

        // Checking caches disabled during checkpointing on wal enabling
        AffinityTopologyVersion top = ctx1.discovery().topologyVersionEx();

        ctx1.exchange().affinityReadyFuture(
            new AffinityTopologyVersion(top.topologyVersion(), top.minorTopologyVersion() + 1))
            .listen(new CI1<Object>() {
                @Override public void apply(Object o) {
                    IgniteCache cache = ignite1.getOrCreateCache(CACHE1);

                    try {
                        cache.put(-1, -1);

                        fail("Should be restricted. This test can te flaky in case of super fast checkpointing.");
                    }
                    catch (Exception e) {
                        assertTrue(e.getMessage(), e.getMessage().contains("disabled"));

                        opsRestricted.countDown();
                    }
                }
            });

        ctxClient.exchange().affinityReadyFuture(
            new AffinityTopologyVersion(top.topologyVersion(), top.minorTopologyVersion() + 1))
            .listen(new CI1<Object>() {
                @Override public void apply(Object o) {
                    IgniteCache cache = client3.getOrCreateCache(CACHE1);

                    try {
                        cache.put(-2, -2);

                        fail("Should be restricted. This test can te flaky in case of super fast checkpointing.");
                    }
                    catch (Exception e) {
                        assertTrue(e.getMessage(), e.getMessage().contains("disabled"));

                        opsRestricted.countDown();
                    }
                }
            });

        client3.cluster().enableWal(CACHE1);

        assert opsRestricted.await(5, TimeUnit.SECONDS);

        // Make sure everything persisted on WAL enabling
        stopAllGrids(true);

        final Ignite newIgnite1 = startGrid(1);
        final Ignite newIgnite2 = startGrid(2);
        final Ignite newClient3 = startGrid(3);
        final Ignite newIgnite4 = startGrid(4);

        newClient3.active(true);

        final IgniteCache cache = newClient3.getOrCreateCache(CACHE1);

        assertEquals(size, cache.size());

        assertTrue(newClient3.cluster().isWalEnabled(CACHE1));
    }

    /**
     *
     */
    public void testFailDuringStreaming() throws Exception {
        testFailDuringStreaming(false);
    }

    /**
     *
     */
    public void testFailDuringStreamingWithJoin() throws Exception {
        testFailDuringStreaming(true);
    }

    /**
     *
     */
    private void testFailDuringStreaming(boolean join) throws Exception {
        final IgniteEx ignite1 = startGrid(1);
        final IgniteEx ignite2 = startGrid(2);

        ignite1.active(true);

        ignite1.cluster().disableWal(CACHE1);

        // Streaming
        final IgniteCache cache1 = ignite1.getOrCreateCache(CACHE1);

        int size = 100_000;

        for (int i = 0; i < size; i++)
            cache1.put(i, i);

        assertEquals(size, cache1.size());

        IgniteInternalFuture cpFut1 = ignite1.context().cache().context().database().wakeupForCheckpoint("test");
        IgniteInternalFuture cpFut2 = ignite2.context().cache().context().database().wakeupForCheckpoint("test");

        cpFut1.get();
        cpFut2.get();

        for (int i = size; i < size + size; i++)
            cache1.put(i, i);

        for (Ignite node : G.allGrids())
            assertFalse(((IgniteEx)node).context().cache().context().pageStore().walDisabledGroups().isEmpty());

        stopAllGrids(true);

        Ignite ignite = startGrid(1);

        if (!join)
            startGrid(2);

        ignite.active(true);

        if (join)
            startGrid(2);

        awaitPartitionMapExchange();

        final IgniteCache cache = ignite.getOrCreateCache(CACHE1);

        assertEquals(0, cache.size());

        for (final Ignite node : G.allGrids())
            assertTrue(((IgniteEx)node).context().cache().context().pageStore().walDisabledGroups().isEmpty());
    }

    /**
     *
     */
    public void testConcurrent() throws Exception {
        testAlreadyDone(false, true);
        testAlreadyDone(false, false);
    }

    /**
     *
     */
    public void testConcurrentOnAlreadyChanged() throws Exception {
        testAlreadyDone(true, true);
        testAlreadyDone(true, false);
    }

    /**
     *
     */
    private void testAlreadyDone(boolean alreadyChanged, final boolean disable) throws Exception {
        try {
            final IgniteEx ignite1 = startGrid(1);
            final IgniteEx ignite2 = startGrid(2);
            final IgniteEx client3 = startGrid(3);
            final IgniteEx ignite4 = startGrid(4);
            final IgniteEx ignite5 = startGrid(5);

            ignite1.active(true);

            final int size = G.allGrids().size() * G.allGrids().size();

            if (alreadyChanged)
                ignite1.context().cache().changeWalMode(CACHE1, disable, true).get();

            Collection<Thread> threads = new HashSet<>();

            for (int i = 0; i < size; i++) {
                final int finalI = i;

                Thread th = new Thread() {
                    @Override public void run() {
                        IgniteEx ignite = ((IgniteEx)G.allGrids().get(finalI % G.allGrids().size()));

                        ignite.context().cache().changeWalMode(CACHE1, disable, true);
                    }
                };

                threads.add(th);

                th.start();
            }

            for (Thread th : threads)
                th.join();

            checkWal(disable);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    public void testOriginatingNodeLeft() throws Exception {
        testOriginatingLeft(false);
    }

    /**
     *
     */
    public void testOriginatingClientLeft() throws Exception {
        testOriginatingLeft(true);
    }

    /**
     *
     */
    private void testOriginatingLeft(boolean client) throws Exception {
        startGrid(10);
        startGrid(11);
        startGrid(12);
        startGrid(13);
        startGrid(14);

        int igniteId = client ? 3 : 1;

        IgniteEx ignite = startGrid(igniteId);

        if (client)
            assertTrue(ignite.configuration().isClientMode());

        ignite.active(true);

        requestWalModeChangeAndFail(igniteId, true);

        startGrid(igniteId);

        requestWalModeChangeAndFail(igniteId, false);
    }

    /**
     * @param igniteId Ignite id.
     * @param disable Disable.
     */
    private void requestWalModeChangeAndFail(final int igniteId,
        final boolean disable)
        throws Exception {
        final CountDownLatch disableLatch = new CountDownLatch(1);

        Random r = new Random();

        int id1 = r.nextInt(5) + 10;
        int id2;

        do {
            id2 = r.nextInt(5) + 10;
        }
        while (id2 == id1);

        stopGrid(id1, true);

        new Thread() {
            @Override public void run() {
                grid(igniteId).context().cache().changeWalMode(CACHE1, disable, true);

                disableLatch.countDown();
            }
        }.start();

        disableLatch.await();

        stopGrid(igniteId, true);
        stopGrid(id2, true);

        awaitPartitionMapExchange();

        startGrid(id1);
        startGrid(id2);

        final int g1 = grid(id1).context().cache().cacheDescriptor(CACHE1).groupId();

        // Initial message can be lost due to massive nodes failure, but state should be exactly the same at all nodes.
        checkWal(grid(id1).context().cache().context().cache().cacheGroup(g1).walDisabled());
    }

    /**
     *
     */
    private void checkWal(final boolean disable)
        throws IgniteInterruptedCheckedException {

        final int g1 = ((IgniteEx)G.allGrids().get(0)).context().cache().cacheDescriptor(CACHE1).groupId();

        assertTrue(GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for (Ignite node : G.allGrids()) {
                    GridCacheSharedContext ctx = ((IgniteEx)node).context().cache().context();

                    if (ctx.wal() != null && disable != ctx.cache().cacheGroup(g1).walDisabled())
                        return false;
                }

                return true;
            }
        }, 5000));

        assertTrue(GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                try {
                    // Make sure writes allowed
                    for (Ignite node : G.allGrids()) {
                        GridCacheSharedContext ctx = ((IgniteEx)node).context().cache().context();

                        ctx.cache().getOrStartCache(CACHE1).put(1, 1);
                    }
                }
                catch (IgniteCheckedException e) {
                    return false;
                }

                return true;
            }
        }, 5000));

        assertTrue(GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for (Ignite node : G.allGrids()) {
                    GridCacheSharedContext ctx = ((IgniteEx)node).context().cache().context();

                    final Map futs = U.field(ctx.cache(), "walModeChangeFuts");

                    if (!futs.isEmpty())
                        return false;
                }

                return true;
            }
        }, 5000));

    }

    /**
     * Checks internal methods allow to enable/disable more than one cache.
     */
    public void testCacheGroupOps() throws Exception {
        final IgniteEx ignite1 = startGrid(1);
        final IgniteEx ignite2 = startGrid(2);
        final IgniteEx client3 = startGrid(3);

        assertTrue(client3.configuration().isClientMode());

        client3.active(true);

        for (Ignite ignite : G.allGrids()) {
            IgniteEx node = (IgniteEx)ignite;

            checkWal(false, false, false);

            node.context().cache().changeWalMode(CACHE1, true, true).get();

            checkWal(true, false, false);

            node.context().cache().changeWalMode(CACHE1, false, true).get();

            checkWal(false, false, false);

            node.context().cache().changeWalMode(CACHE2, true, false).get();

            checkWal(false, true, true);

            node.context().cache().changeWalMode(CACHE1, true, true).get();

            checkWal(true, true, true);

            node.context().cache().changeWalMode(CACHE1, false, true).get();
            node.context().cache().changeWalMode(CACHE2, false, false).get();

            checkWal(false, false, false);

            try {
                node.context().cache().changeWalMode(CACHE2, true, true).get();

                fail("Should be restricted.");
            }
            catch (Exception e) {
                // No-op.
            }

            checkWal(false, false, false);
        }
    }

    /**
     * @param disabled1 Disabled 1.
     * @param disabled2 Disabled 2.
     * @param disabled3 Disabled 3.
     */
    private void checkWal(
        boolean disabled1,
        boolean disabled2,
        boolean disabled3) throws Exception {
        GridCacheSharedContext ctx0 = ((IgniteEx)G.allGrids().get(0)).context().cache().context();

        int cnt = ctx0.cache().getOrStartCache(CACHE1).size();

        final int g1 = ctx0.cache().cacheDescriptor(CACHE1).groupId();
        final int g2 = ctx0.cache().cacheDescriptor(CACHE2).groupId();
        final int g3 = ctx0.cache().cacheDescriptor(CACHE3).groupId();

        assertEquals(g2, g3);

        for (Ignite node : G.allGrids()) {
            GridCacheSharedContext ctx = ((IgniteEx)node).context().cache().context();

            if (ctx.wal() != null) {
                assertEquals(disabled1, ctx.cache().cacheGroup(g1).walDisabled());
                assertEquals(disabled2, ctx.cache().cacheGroup(g2).walDisabled());
                assertEquals(disabled3, ctx.cache().cacheGroup(g3).walDisabled());
            }
        }

        // Make sure writes allowed
        for (Ignite node : G.allGrids()) {
            GridCacheSharedContext ctx = ((IgniteEx)node).context().cache().context();

            ctx.cache().getOrStartCache(CACHE1).put(++cnt, cnt);
            ctx.cache().getOrStartCache(CACHE2).put(++cnt, cnt);
            ctx.cache().getOrStartCache(CACHE3).put(++cnt, cnt);
        }

        for (Ignite node : G.allGrids()) {
            GridCacheSharedContext ctx = ((IgniteEx)node).context().cache().context();

            final Map futs = U.field(ctx.cache(), "walModeChangeFuts");

            assertTrue(GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return futs.isEmpty();
                }
            }, 1000));
        }
    }
}
