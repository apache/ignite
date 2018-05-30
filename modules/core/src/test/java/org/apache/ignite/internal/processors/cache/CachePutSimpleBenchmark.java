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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 *
 */
public class CachePutSimpleBenchmark extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "test";

    /** */
    private static final int SRVS = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCacheConfiguration(cacheConfigurations());

//        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
//            new DataRegionConfiguration().setName("asd").setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(SRVS);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60 * 60_000;
    }

    /**
     * @return Cache configurations.
     */
    private CacheConfiguration[] cacheConfigurations() {
        List<CacheConfiguration> ccfgs = new ArrayList<>();

        ccfgs.add(cacheConfiguration(PARTITIONED, PRIMARY_SYNC, 1));

        return ccfgs.toArray(new CacheConfiguration[ccfgs.size()]);
    }

    /**
     * @param cacheMode Cache mode.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(
        CacheMode cacheMode,
        CacheWriteSynchronizationMode syncMode,
        int backups) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        final int KEYS = 100_000;

        final AtomicInteger cnt = new AtomicInteger();

        final IgniteCache<Integer, Integer> cache = ignite(0).cache(CACHE_NAME);

        while (true) {
            cnt.set(0);
            cache.clear();

            long t0 = System.currentTimeMillis();

            GridTestUtils.runMultiThreaded(new Runnable() {
                @Override public void run() {
                    while (cnt.get() < KEYS) {
                        Integer key = ThreadLocalRandom.current().nextInt();

                        cache.put(key, key);

                        cnt.incrementAndGet();
                    }
                }
            }, 10, "put");

            System.out.println("+++ " + (System.currentTimeMillis() - t0));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAll() throws Exception {
        final int KEYS = 100_000;

        ignite(0).cluster().active(true);

        final AtomicInteger cnt = new AtomicInteger();

        final IgniteCache<Integer, Integer> cache = ignite(0).cache(CACHE_NAME);

        while (true) {
            cnt.set(0);
            cache.clear();

            long t0 = System.currentTimeMillis();

            GridTestUtils.runMultiThreaded(new Runnable() {
                @Override public void run() {
                    while (cnt.get() < KEYS) {
                        Map<Integer, Integer> m = new HashMap<>();

                        for (int i = 0; i < 1000; ++i) {
                            Integer key = ThreadLocalRandom.current().nextInt();

                            cache.put(key, key);
                        }

                        cache.putAll(m);

                        cnt.addAndGet(1000);
                    }
                }
            }, 10, "put");

            System.out.println("+++ " + (System.currentTimeMillis() - t0));
        }
    }
}
