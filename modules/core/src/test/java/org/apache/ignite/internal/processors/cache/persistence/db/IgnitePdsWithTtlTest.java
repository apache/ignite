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

import com.google.common.base.Strings;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
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
    public static final String CACHE = "expirableCache";

    /** */
    private static final int EXPIRATION_TIMEOUT = 10;

    /** */
    public static final int ENTRIES = 7000;

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
        ccfg.setName(CACHE);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 128));
        ccfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, EXPIRATION_TIMEOUT)));
        ccfg.setEagerTtl(true);
        ccfg.setGroupName("group1");

        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(256L * 1024 * 1024)
                        .setPersistenceEnabled(true)
                ).setWalMode(WALMode.DEFAULT));

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

        fillCache(srv.cache(CACHE));

        if (restartGrid) {
            stopGrid(0);
            srv = startGrid(0);
            srv.cluster().active(true);
        }

        final IgniteCache<Integer, String> cache = srv.cache(CACHE);

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

        fillCache(srv.cache(CACHE));

        //causes rebalancing start
        srv = startGrid(1);

        final IgniteCache<Integer, String> cache = srv.cache(CACHE);

        pringStatistics((IgniteCacheProxy)cache, "After rebalancing start");

        waitAndCheckExpired(cache);

        stopAllGrids();
    }

    /** */
    protected void fillCache(IgniteCache<Integer, String> cache) {
        cache.putAll(new TreeMap<Integer, String>() {{
            for (int i = 0; i < ENTRIES; i++)
                put(i, Strings.repeat("Some value " + i, 125));
        }});

        //Touch entries.
        for (int i = 0; i < ENTRIES; i++)
            cache.get(i); // touch entries

        pringStatistics((IgniteCacheProxy)cache, "After cache puts");
    }

    /** */
    protected void waitAndCheckExpired(final IgniteCache<Integer, String> cache) throws IgniteInterruptedCheckedException {
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