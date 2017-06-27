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

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
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
public class IgniteActiveClusterTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** */
    private boolean active = true;

    /** */
    private CacheConfiguration ccfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        cfg.setActiveOnStart(active);

        if (ccfg != null) {
            cfg.setCacheConfiguration(ccfg);

            ccfg = null;
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivate() throws Exception {
        active = false;

        for (int i = 0; i < 3; i++) {
            ccfg = cacheConfiguration(DEFAULT_CACHE_NAME);

            startGrid(i);
        }

        ignite(0).active(true);

        startGrid(3);

        for (int i  = 0; i < 4; i++) {
            IgniteCache<Integer, Integer> cache = ignite(i).cache(DEFAULT_CACHE_NAME);

            for (int j = 0; j < 10; j++) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                Integer key = rnd.nextInt(1000);

                cache.put(key, j);

                assertEquals((Integer)j, cache.get(key));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinAndActivate() throws Exception {
        for (int iter = 0; iter < 3; iter++) {
            log.info("Iteration: " + iter);

            active = false;

            for (int i = 0; i < 3; i++) {
                ccfg = cacheConfiguration(DEFAULT_CACHE_NAME);

                startGrid(i);
            }

            final int START_NODES = 3;

            final CyclicBarrier b = new CyclicBarrier(START_NODES + 1);

            IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    b.await();

                    Thread.sleep(ThreadLocalRandom.current().nextLong(100) + 1);

                    ignite(0).active(true);

                    return null;
                }
            });

            final AtomicInteger nodeIdx = new AtomicInteger(3);

            IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int idx = nodeIdx.getAndIncrement();

                    b.await();

                    startGrid(idx);

                    return null;
                }
            }, START_NODES, "start-node");

            fut1.get();
            fut2.get();

            for (int i  = 0; i < 6; i++) {
                IgniteCache<Integer, Integer> cache = ignite(i).cache(DEFAULT_CACHE_NAME);

                for (int j = 0; j < 10; j++) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Integer key = rnd.nextInt(1000);

                    cache.put(key, j);

                    assertEquals((Integer)j, cache.get(key));
                }
            }

            stopAllGrids();
        }
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(3);

        return ccfg;
    }
}
