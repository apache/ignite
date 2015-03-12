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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 */
public class GridCacheDhtPreloadPutGetSelfTest extends GridCommonAbstractTest {
    /** Key count. */
    private static final int KEY_CNT = 1000;

    /** Iterations count. */
    private static final int ITER_CNT = 10;

    /** Frequency. */
    private static final int FREQUENCY = 100;

    /** Number of key backups. Each test method can set this value as required. */
    private int backups;

    /** Preload mode. */
    private CacheRebalanceMode preloadMode;

    /** IP finder. */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        assert preloadMode != null;

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setRebalanceMode(preloadMode);
        cacheCfg.setBackups(backups);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);
        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetAsync0() throws Exception {
        preloadMode = ASYNC;
        backups = 0;

        performTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetAsync1() throws Exception {
        preloadMode = ASYNC;
        backups = 1;

        performTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetAsync2() throws Exception {
        preloadMode = ASYNC;
        backups = 2;

        performTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetSync0() throws Exception {
        preloadMode = SYNC;
        backups = 0;

        performTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetSync1() throws Exception {
        preloadMode = SYNC;
        backups = 1;

        performTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetSync2() throws Exception {
        preloadMode = SYNC;
        backups = 2;

        performTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetNone0() throws Exception {
        preloadMode = NONE;
        backups = 0;

        performTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetNone1() throws Exception {
        preloadMode = NONE;
        backups = 1;

        performTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetNone2() throws Exception {
        preloadMode = NONE;
        backups = 2;

        performTest();
    }

    /**
     * @throws Exception If test fails.
     */
    private void performTest() throws Exception {
        try {
            final CountDownLatch writeLatch = new CountDownLatch(1);

            final CountDownLatch readLatch = new CountDownLatch(1);

            final AtomicBoolean done = new AtomicBoolean();

            IgniteInternalFuture fut1 = GridTestUtils.runMultiThreadedAsync(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        Ignite g2 = startGrid(2);

                        for (int i = 0; i < ITER_CNT; i++) {
                            info("Iteration # " + i);

                            IgniteCache<Integer, Integer> cache = g2.jcache(null);

                            for (int j = 0; j < KEY_CNT; j++) {
                                Integer val = cache.get(j);

                                if (j % FREQUENCY == 0)
                                    info("Read entry: " + j + " -> " + val);

                                if (done.get())
                                    assert val != null && val == j;
                            }

                            writeLatch.countDown();

                            readLatch.await();
                        }

                        return null;
                    }
                },
                1,
                "reader"
            );

            IgniteInternalFuture fut2 = GridTestUtils.runMultiThreadedAsync(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        try {
                            writeLatch.await(10, TimeUnit.SECONDS);

                            Ignite g1 = startGrid(1);

                            IgniteCache<Integer, Integer> cache = g1.jcache(null);

                            for (int j = 0; j < KEY_CNT; j++) {
                                cache.put(j, j);

                                if (j % FREQUENCY == 0)
                                    info("Stored value in cache: " + j);
                            }

                            done.set(true);

                            for (int j = 0; j < KEY_CNT; j++) {
                                Cache.Entry<Integer, Integer> entry = internalCache(cache).entry(j);

                                assert entry != null;

                                Integer val = entry.getValue();

                                if (j % FREQUENCY == 0)
                                    info("Read entry: " + entry.getKey() + " -> " + val);

                                assert val != null && val == j;
                            }

                            if (backups > 0)
                                stopGrid(1);
                        }
                        finally {
                            readLatch.countDown();
                        }

                        return null;
                    }
                },
                1,
                "writer"
            );

            fut1.get();
            fut2.get();
        }
        finally {
            stopAllGrids();
        }
    }
}
