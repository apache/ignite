/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test TTL worker with persistence enabled
 */
public class IgnitePdsWithTtlTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "expirableCache";

    /** */
    private static final int EXPIRATION_TIMEOUT = 10;

    /** */
    public static final int ENTRIES = 100_000;

    /** */
    private static final TcpDiscoveryVmIpFinder FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //protection if test failed to finish, e.g. by error
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(FINDER);

        cfg.setDiscoverySpi(disco);

        final CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(CACHE_NAME);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, EXPIRATION_TIMEOUT)));
        ccfg.setEagerTtl(true);
        ccfg.setGroupName("group1");
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(192L * 1024 * 1024)
                        .setPersistenceEnabled(true)
                ).setWalMode(WALMode.LOG_ONLY));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testTtlIsApplied() throws Exception {
        loadAndWaitForCleanup(false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testTtlIsAppliedAfterRestart() throws Exception {
        loadAndWaitForCleanup(true);
    }

    /**
     * @throws Exception if failed.
     */
    private void loadAndWaitForCleanup(boolean restartGrid) throws Exception {
        IgniteEx srv = startGrid(0);
        srv.cluster().active(true);

        fillCache(srv.cache(CACHE_NAME));

        if (restartGrid) {
            stopGrid(0);
            srv = startGrid(0);
            srv.cluster().active(true);
        }

        final IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME);

        pringStatistics((IgniteCacheProxy)cache, "After restart from LFS");

        waitAndCheckExpired(cache);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingWithTtlExpirable() throws Exception {
        IgniteEx srv = startGrid(0);
        srv.cluster().active(true);

        fillCache(srv.cache(CACHE_NAME));

        srv = startGrid(1);

        //causes rebalancing start
        srv.cluster().setBaselineTopology(srv.cluster().topologyVersion());

        final IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME);

        pringStatistics((IgniteCacheProxy)cache, "After rebalancing start");

        waitAndCheckExpired(cache);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    public void testStartStopAfterRebalanceWithTtlExpirable() throws Exception {
        try {
            IgniteEx srv = startGrid(0);
            startGrid(1);
            srv.cluster().active(true);

            ExpiryPolicy plc = CreatedExpiryPolicy.factoryOf(Duration.ONE_DAY).create();

            IgniteCache<Integer, byte[]> cache0 = srv.cache(CACHE_NAME);

            fillCache(cache0.withExpiryPolicy(plc));

            srv = startGrid(2);

            IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME);

            //causes rebalancing start
            srv.cluster().setBaselineTopology(srv.cluster().topologyVersion());

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return Boolean.TRUE.equals(cache.rebalance().get()) && cache.localSizeLong(CachePeekMode.ALL) > 0;
                }
            }, 20_000);

            //check if pds is consistent
            stopGrid(0);
            startGrid(0);

            stopGrid(1);
            startGrid(1);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    protected void fillCache(IgniteCache<Integer, byte[]> cache) {
        cache.putAll(new TreeMap<Integer, byte[]>() {{
            for (int i = 0; i < ENTRIES; i++)
                put(i, new byte[1024]);
        }});

        //Touch entries.
        for (int i = 0; i < ENTRIES; i++)
            cache.get(i); // touch entries

        pringStatistics((IgniteCacheProxy)cache, "After cache puts");
    }

    /** */
    protected void waitAndCheckExpired(
        final IgniteCache<Integer, byte[]> cache) throws IgniteInterruptedCheckedException {
        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return cache.size() == 0;
            }
        }, TimeUnit.SECONDS.toMillis(EXPIRATION_TIMEOUT + 1));

        pringStatistics((IgniteCacheProxy)cache, "After timeout");

        for (int i = 0; i < ENTRIES; i++)
            assertNull(cache.get(i));
    }

    /** */
    private void pringStatistics(IgniteCacheProxy cache, String msg) {
        System.out.println(msg + " {{");
        cache.context().printMemoryStats();
        System.out.println("}} " + msg);
    }
}