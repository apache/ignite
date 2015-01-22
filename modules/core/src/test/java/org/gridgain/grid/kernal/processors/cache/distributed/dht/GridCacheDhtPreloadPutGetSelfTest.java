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

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCachePreloadMode.*;
import static org.apache.ignite.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 *
 * Forum example <a
 * href="http://www.gridgainsystems.com/jiveforums/thread.jspa?threadID=1449">
 * http://www.gridgainsystems.com/jiveforums/thread.jspa?threadID=1449</a>
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
    private GridCachePreloadMode preloadMode;

    /** IP finder. */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        assert preloadMode != null;

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setPreloadMode(preloadMode);
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

            IgniteFuture fut1 = GridTestUtils.runMultiThreadedAsync(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        Ignite g2 = startGrid(2);

                        for (int i = 0; i < ITER_CNT; i++) {
                            info("Iteration # " + i);

                            GridCache<Integer, Integer> cache = g2.cache(null);

                            for (int j = 0; j < KEY_CNT; j++) {
                                GridCacheEntry<Integer, Integer> entry = cache.entry(j);

                                assert entry != null;

                                Integer val = entry.getValue();

                                if (j % FREQUENCY == 0)
                                    info("Read entry: " + entry.getKey() + " -> " + val);

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

            IgniteFuture fut2 = GridTestUtils.runMultiThreadedAsync(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        writeLatch.await();

                        Ignite g1 = startGrid(1);

                        GridCache<Integer, Integer> cache = g1.cache(null);

                        for (int j = 0; j < KEY_CNT; j++) {
                            cache.put(j, j);

                            if (j % FREQUENCY == 0)
                                info("Stored value in cache: " + j);
                        }

                        done.set(true);

                        for (int j = 0; j < KEY_CNT; j++) {
                            GridCacheEntry<Integer, Integer> entry = cache.entry(j);

                            assert entry != null;

                            Integer val = entry.getValue();

                            if (j % FREQUENCY == 0)
                                info("Read entry: " + entry.getKey() + " -> " + val);

                            assert val != null && val == j;
                        }

                        if (backups > 0)
                            stopGrid(1);

                        readLatch.countDown();

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
