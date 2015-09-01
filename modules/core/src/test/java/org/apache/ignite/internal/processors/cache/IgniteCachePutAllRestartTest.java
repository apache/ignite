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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCachePutAllRestartTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /** IP finder. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cacheCfg.setNearConfiguration(null);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopNode() throws Exception {
        startGrids(NODES);

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("put-thread");

                IgniteCache<Integer, Integer> cache = ignite(0).cache(CACHE_NAME);

                Random rnd = new Random();

                int iter = 0;

                while (!stop.get()) {
                    Map<Integer, Integer> map = new HashMap<>();

                    for (int i = 0; i < 10; i++)
                        map.put(rnd.nextInt(1000), i);

                    try {
                        cache.putAll(map);
                    }
                    catch (CacheException e) {
                        log.info("Update failed: " + e);
                    }

                    iter++;

                    if (iter % 10 == 0)
                        log.info("Iteration: " + iter);
                }

                return null;
            }
        });

        try {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            long endTime = System.currentTimeMillis() + 2 * 60_000;

            while (System.currentTimeMillis() < endTime) {
                int node = rnd.nextInt(1, NODES);

                stopGrid(node);

                startGrid(node);
            }
        }
        finally {
            stop.set(true);
        }

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopOriginatingNode() throws Exception {
        startGrids(NODES);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long endTime = System.currentTimeMillis() + 2 * 60_000;

        while (System.currentTimeMillis() < endTime) {
            int node = rnd.nextInt(0, NODES);

            final Ignite ignite = ignite(node);

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Thread.currentThread().setName("put-thread");

                    IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

                    Random rnd = new Random();

                    long endTime = System.currentTimeMillis() + 60_000;

                    try {
                        int iter = 0;

                        while (System.currentTimeMillis() < endTime) {
                            Map<Integer, Integer> map = new HashMap<>();

                            for (int i = 0; i < 10; i++)
                                map.put(rnd.nextInt(1000), i);

                            cache.putAll(map);

                            iter++;

                            log.info("Iteration: " + iter);
                        }

                        fail("Should fail.");
                    }
                    catch (CacheException | IllegalStateException e) {
                        log.info("Expected error: " + e);
                    }

                    return null;
                }
            });

            ignite.close();

            fut.get();

            startGrid(node);
        }
    }
}