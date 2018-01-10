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
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
public class WalModeChangeFailoverSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Cache 1. */
    private static final String CACHE1 = "cache1";

    /** Cache 2. */
    private static final String CACHE2 = "cache2";

    /** Cache 3. */
    private static final String CACHE3 = "cache3";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(3).equals(igniteInstanceName))
            iCfg.setClientMode(true);

        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setIpFinder(ipFinder);

        DataStorageConfiguration dsCfg =
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(1024 * 1024 * 1024));

        iCfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration cache1 = new CacheConfiguration(CACHE1);
        CacheConfiguration cache2 = new CacheConfiguration(CACHE2).setGroupName("group");
        CacheConfiguration cache3 = new CacheConfiguration(CACHE3).setGroupName("group");

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

        assertTrue(ctx1.cache().cacheGroup(g1).walEnabled());
        assertTrue(ctx1.cache().cacheGroup(g2).walEnabled());
        assertTrue(ctx1.cache().cacheGroup(g3).walEnabled());

        assertTrue(ignite1.cluster().isWalEnabled(CACHE1));
        assertTrue(ignite2.cluster().isWalEnabled(CACHE1));
        assertTrue(client3.cluster().isWalEnabled(CACHE1));

        client3.cluster().walDisable(CACHE1);

        assertFalse(ignite1.cluster().isWalEnabled(CACHE1));
        assertFalse(ignite2.cluster().isWalEnabled(CACHE1));
        assertFalse(client3.cluster().isWalEnabled(CACHE1));

        // Checking newcomer know that wal disabled
        Ignite ignite4 = startGrid(4);

        GridCacheSharedContext ctx4 = ((IgniteKernal)ignite4).context().cache().context();

        assertFalse(ctx4.cache().cacheGroup(g1).walEnabled());
        assertTrue(ctx4.cache().cacheGroup(g2).walEnabled());
        assertTrue(ctx4.cache().cacheGroup(g3).walEnabled());

        // Checking old node
        assertFalse(ctx1.cache().cacheGroup(g1).walEnabled());
        assertTrue(ctx1.cache().cacheGroup(g2).walEnabled());
        assertTrue(ctx1.cache().cacheGroup(g3).walEnabled());

        // Streaming
        final IgniteCache cache1 = client3.getOrCreateCache(CACHE1);

        int size = 100_000;

        for (int i = 0; i < size; i++)
            cache1.put(i, i);

        assertEquals(size, cache1.size());

        client3.cluster().walEnable(CACHE1);

        // Make sure everything persisted on WAL enabling
        stopAllGrids(true);

        final Ignite newIgnite1 = startGrid(1);
        final Ignite newIgnite2 = startGrid(2);
        final Ignite newClient3 = startGrid(3);
        final Ignite newIgnite4 = startGrid(4);

        newClient3.active(true);

        final IgniteCache cache = newClient3.getOrCreateCache(CACHE1);

        assertTrue(newClient3.cluster().isWalEnabled(CACHE1));

        assertEquals(size, cache.size());
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

        ignite1.cluster().walDisable(CACHE1);

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
        final IgniteEx ignite1 = startGrid(1);
        final IgniteEx ignite2 = startGrid(2);
        final IgniteEx client3 = startGrid(3);
        final IgniteEx ignite4 = startGrid(4);
        final IgniteEx ignite5 = startGrid(5);

        ignite1.active(true);

        testAlreadyDone(ignite1, false, false);
        testAlreadyDone(ignite1, false, true);

        testAlreadyDone(ignite1, true, false);
        testAlreadyDone(ignite1, true, true);
    }

    /**
     *
     */
    private void testAlreadyDone(IgniteEx ignite1, boolean alreadyChanged, final boolean enable) throws Exception {
        final int size = G.allGrids().size() * G.allGrids().size();

        if (alreadyChanged)
            assertTrue(ignite1.context().cache().changeWalMode(Collections.singleton(CACHE1), enable).get());

        Collection<Thread> threads = new HashSet<>();

        final AtomicReference<IgniteCheckedException> err = new AtomicReference();
        final AtomicInteger fails = new AtomicInteger();

        for (int i = 0; i < size; i++) {
            final int finalI = i;

            Thread th = new Thread() {
                @Override public void run() {
                    IgniteEx ignite = ((IgniteEx)G.allGrids().get(finalI % G.allGrids().size()));

                    try {
                        if (!ignite.context().cache().changeWalMode(Collections.singleton(CACHE1), enable).get())
                            fails.incrementAndGet();
                    }
                    catch (IgniteCheckedException e) {
                        err.set(e);
                    }
                }
            };

            threads.add(th);

            th.start();
        }

        for (Thread th : threads)
            th.join();

        if (err.get() != null)
            throw err.get();

        assertEquals(alreadyChanged ? size : size - 1, fails.get());

        checkWal(enable);
    }

    /**
     *
     */
    public void testWalModeDisableDuringFails() throws Exception {
        testWalModeChangeDuringFails(false, true);
    }

    /**
     *
     */
    public void testWalModeDisableFromClientDuringFails() throws Exception {
        testWalModeChangeDuringFails(true, true);
    }

    /**
     *
     */
    public void testWalModeEnableDuringFails() throws Exception {
        testWalModeChangeDuringFails(false, false);
    }

    /**
     *
     */
    public void testWalModeEnableFromClientDuringFails() throws Exception {
        testWalModeChangeDuringFails(true, false);
    }

    /**
     *
     */
    private void testWalModeChangeDuringFails(boolean client, boolean disable) throws Exception {
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

        if (disable)
            requestWalModeChangeDuringFails(igniteId, false);
        else {
            ignite.cluster().walDisable(CACHE1);

            requestWalModeChangeDuringFails(igniteId, true);
        }
    }

    /**
     * @param igniteId Ignite id.
     * @param enable Disable.
     */
    private void requestWalModeChangeDuringFails(final int igniteId,
        final boolean enable)
        throws Exception {
        final CountDownLatch preparedLatch = new CountDownLatch(1);
        final CountDownLatch startedLatch = new CountDownLatch(1);
        final CountDownLatch finishedLatch = new CountDownLatch(1);

        Random r = new Random();

        int id1 = r.nextInt(5) + 10;
        int id2;

        do {
            id2 = r.nextInt(5) + 10;
        }
        while (id2 == id1);

        new Thread() {
            @Override public void run() {
                try {
                    preparedLatch.await();
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

                IgniteInternalFuture fut =
                    grid(igniteId).context().cache().changeWalMode(Collections.singleton(CACHE1), enable);

                startedLatch.countDown();

                try {
                    fut.get();
                }
                catch (Exception ignored) {
                    // No-op.
                }

                finishedLatch.countDown();
            }
        }.start();

        stopGrid(id1, true);

        preparedLatch.countDown();

        startedLatch.await();

        stopGrid(id2, true);

        startGrid(20);
        startGrid(21);

        finishedLatch.await();

        checkWal(enable);
    }

    /**
     *
     */
    private void checkWal(final boolean enable)
        throws IgniteInterruptedCheckedException {

        final int g1 = ((IgniteEx)G.allGrids().get(0)).context().cache().cacheDescriptor(CACHE1).groupId();

        assertTrue(GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for (Ignite node : G.allGrids()) {
                    GridCacheSharedContext ctx = ((IgniteEx)node).context().cache().context();

                    if (ctx.wal() != null && enable != ctx.cache().cacheGroup(g1).walEnabled())
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

        Set<String> grp = new HashSet<>();

        grp.add(CACHE2);
        grp.add(CACHE3);

        for (Ignite ignite : G.allGrids()) {
            IgniteEx node = (IgniteEx)ignite;

            checkWal(true, true, true);

            node.context().cache().changeWalMode(Collections.singleton(CACHE1), false).get();

            checkWal(false, true, true);

            node.context().cache().changeWalMode(Collections.singleton(CACHE1), true).get();

            checkWal(true, true, true);

            node.context().cache().changeWalMode(grp, false).get();

            checkWal(true, false, false);

            node.context().cache().changeWalMode(Collections.singleton(CACHE1), false).get();

            checkWal(false, false, false);

            node.context().cache().changeWalMode(Collections.singleton(CACHE1), true).get();
            node.context().cache().changeWalMode(grp, true).get();

            checkWal(true, true, true);

            try {
                node.context().cache().changeWalMode(Collections.singleton(CACHE2), false).get();

                fail("Should be restricted.");
            }
            catch (Exception e) {
                // No-op.
            }

            checkWal(true, true, true);
        }
    }

    /**
     * @param enabled1 Disabled 1.
     * @param enabled2 Disabled 2.
     * @param enabled3 Disabled 3.
     */
    private void checkWal(
        boolean enabled1,
        boolean enabled2,
        boolean enabled3) {
        GridCacheSharedContext ctx0 = ((IgniteEx)G.allGrids().get(0)).context().cache().context();

        final int g1 = ctx0.cache().cacheDescriptor(CACHE1).groupId();
        final int g2 = ctx0.cache().cacheDescriptor(CACHE2).groupId();
        final int g3 = ctx0.cache().cacheDescriptor(CACHE3).groupId();

        assertEquals(g2, g3);

        for (Ignite node : G.allGrids()) {
            GridCacheSharedContext ctx = ((IgniteEx)node).context().cache().context();

            if (ctx.wal() != null) {
                assertEquals(enabled1, ctx.cache().cacheGroup(g1).walEnabled());
                assertEquals(enabled2, ctx.cache().cacheGroup(g2).walEnabled());
                assertEquals(enabled3, ctx.cache().cacheGroup(g3).walEnabled());
            }
        }
    }
}
