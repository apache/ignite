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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Checks stop and destroy methods behavior.
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
    private static String REGION = "region";

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
        GridCacheSharedContext ctx2 = ((IgniteKernal)ignite2).context().cache().context();
        GridCacheSharedContext ctxClient = ((IgniteKernal)client3).context().cache().context();

        final int g1 = ctx1.cache().cacheDescriptor(CACHE1).groupId();
        final int g2 = ctx1.cache().cacheDescriptor(CACHE2).groupId();
        final int g3 = ctx1.cache().cacheDescriptor(CACHE3).groupId();

        assertFalse(ctx1.wal().disabled(g1));
        assertFalse(ctx1.wal().disabled(g2));
        assertFalse(ctx1.wal().disabled(g3));

        client3.cluster().disableWal(Collections.singleton(CACHE1));

        // Checking newcomer know that wal disabled
        Ignite ignite4 = startGrid(4);

        GridCacheSharedContext ctx4 = ((IgniteKernal)ignite4).context().cache().context();

        assertTrue(ctx4.wal().disabled(g1));
        assertFalse(ctx4.wal().disabled(g2));
        assertFalse(ctx4.wal().disabled(g3));

        // Checking old node
        assertTrue(ctx1.wal().disabled(g1));
        assertFalse(ctx1.wal().disabled(g2));
        assertFalse(ctx1.wal().disabled(g3));

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

        client3.cluster().enableWal(Collections.singleton(CACHE1));

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
    }

    /**
     *
     */
    public void testAlreadyDoneConcurrent() throws Exception {
        testAlreadyDone(false);
    }

    /**
     *
     */
    public void testAlreadyDonePredisabled() throws Exception {
        testAlreadyDone(true);
    }

    /**
     *
     */
    private void testAlreadyDone(boolean alreadyDisabled) throws Exception {
        final IgniteEx ignite = startGrid(1);

        ignite.active(true);

        final int size = 10;

        final AtomicInteger duplicates = new AtomicInteger();

        if (alreadyDisabled)
            ignite.cluster().disableWal(Collections.singleton(CACHE1));

        Collection<Thread> threads = new HashSet<>();

        for (int i = 0; i < size; i++) {
            Thread th = new Thread() {
                @Override public void run() {
                    IgniteInternalFuture fut =
                        ignite.context().cache().changeWalMode(
                            Collections.singleton(CACHE1),
                            true,
                            true);

                    if (fut instanceof GridFinishedFuture)
                        duplicates.incrementAndGet();
                }
            };

            threads.add(th);

            th.start();
        }

        for (Thread th : threads)
            th.join();

        assertEquals(alreadyDisabled ? size : size - 1, duplicates.get());
    }

    /**
     *
     */
    public void testCacheGroupsOps() throws Exception {
        final Ignite ignite1 = startGrid(1);
        final Ignite ignite2 = startGrid(2);
        final Ignite client3 = startGrid(3);

        assertTrue(client3.configuration().isClientMode());

        client3.active(true);

        GridCacheSharedContext ctx1 = ((IgniteKernal)ignite1).context().cache().context();
        GridCacheSharedContext ctx2 = ((IgniteKernal)ignite2).context().cache().context();
        GridCacheSharedContext ctx3 = ((IgniteKernal)client3).context().cache().context();

        Collection<GridCacheSharedContext> ctxs = new HashSet<>();

        ctxs.add(ctx1);
        ctxs.add(ctx2);
        ctxs.add(ctx3);

        Collection<Ignite> ignites = new LinkedHashSet<>();

        ignites.add(ignite1);
        ignites.add(ignite2);
        ignites.add(client3);

        for (Ignite ignite : ignites) {
            checkWal(ctxs, false, false, false);

            ignite.cluster().disableWal(Collections.singleton(CACHE1));

            checkWal(ctxs, true, false, false);

            ignite.cluster().enableWal(Collections.singleton(CACHE1));

            checkWal(ctxs, false, false, false);

            ignite.cluster().disableWal(Collections.singleton(CACHE2), false);

            checkWal(ctxs, false, true, true);

            ignite.cluster().disableWal(Collections.singleton(CACHE1));

            checkWal(ctxs, true, true, true);

            ignite.cluster().enableWal(Collections.singleton(CACHE1));
            ignite.cluster().enableWal(Collections.singleton(CACHE2), false);

            checkWal(ctxs, false, false, false);

            final Set<String> both = new HashSet<>();

            both.add(CACHE1);
            both.add(CACHE2);

            ignite.cluster().disableWal(both, false);

            checkWal(ctxs, true, true, true);

            final Set<String> all = new HashSet<>();

            all.add(CACHE1);
            all.add(CACHE2);
            all.add(CACHE3);

            ignite.cluster().enableWal(all, false);

            checkWal(ctxs, false, false, false);
        }
    }

    /**
     * @param ctxs Ctxs.
     * @param disabled1 Disabled 1.
     * @param disabled2 Disabled 2.
     * @param disabled3 Disabled 3.
     */
    private void checkWal(Collection<GridCacheSharedContext> ctxs,
        boolean disabled1,
        boolean disabled2,
        boolean disabled3) throws Exception {
        GridCacheSharedContext ctx0 = ctxs.iterator().next();

        int cnt = ctx0.cache().getOrStartCache(CACHE1).size();

        final int g1 = ctx0.cache().cacheDescriptor(CACHE1).groupId();
        final int g2 = ctx0.cache().cacheDescriptor(CACHE2).groupId();
        final int g3 = ctx0.cache().cacheDescriptor(CACHE3).groupId();

        assertEquals(g2, g3);

        for (GridCacheSharedContext ctx : ctxs) {
            if (ctx.wal() != null) {
                assertEquals(disabled1, ctx.wal().disabled(g1));
                assertEquals(disabled2, ctx.wal().disabled(g2));
                assertEquals(disabled3, ctx.wal().disabled(g3));
            }
        }

        // Make sure writes allowed
        for (GridCacheSharedContext ctx : ctxs) {
            ctx.cache().getOrStartCache(CACHE1).put(++cnt, cnt);
            ctx.cache().getOrStartCache(CACHE2).put(++cnt, cnt);
            ctx.cache().getOrStartCache(CACHE3).put(++cnt, cnt);
        }

        for (GridCacheSharedContext ctx : ctxs) {
            final Map futs = U.field(ctx.cache(), "walModeChangeFuts");

            assertTrue(GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return futs.isEmpty();
                }
            }, 1000));
        }
    }
}
