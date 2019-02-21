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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheGetRestartTest extends GridCommonAbstractTest {
    /** */
    private static final long TEST_TIME = 60_000;

    /** */
    private static final int SRVS = 3;

    /** */
    private static final int CLIENTS = 1;

    /** */
    private static final int KEYS = 100_000;

    /** */
    private ThreadLocal<Boolean> client = new ThreadLocal<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        Boolean clientMode = client.get();

        if (clientMode != null) {
            cfg.setClientMode(clientMode);

            client.remove();
        }

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(SRVS);

        for (int i = 0; i < CLIENTS; i++) {
            client.set(true);

            Ignite client = startGrid(SRVS);

            assertTrue(client.configuration().isClientMode());
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIME + 3 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetRestartReplicated() throws Exception {
        CacheConfiguration<Object, Object> cache = cacheConfiguration(REPLICATED, 0, false);

        checkRestart(cache, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetRestartPartitioned1() throws Exception {
        CacheConfiguration<Object, Object> cache = cacheConfiguration(PARTITIONED, 1, false);

        checkRestart(cache, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetRestartPartitioned2() throws Exception {
        CacheConfiguration<Object, Object> cache = cacheConfiguration(PARTITIONED, 2, false);

        checkRestart(cache, 2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetRestartPartitionedNearEnabled() throws Exception {
        CacheConfiguration<Object, Object> cache = cacheConfiguration(PARTITIONED, 1, true);

        checkRestart(cache, 1);
    }

    /**
     * @param ccfg Cache configuration.
     * @param restartCnt Number of nodes to restart.
     * @throws Exception If failed.
     */
    private void checkRestart(final CacheConfiguration ccfg, final int restartCnt) throws Exception {
        ignite(0).createCache(ccfg);

        try {
            if (ccfg.getNearConfiguration() != null)
                ignite(SRVS).createNearCache(ccfg.getName(), new NearCacheConfiguration<>());

            try (IgniteDataStreamer<Object, Object> streamer = ignite(0).dataStreamer(ccfg.getName())) {
                streamer.allowOverwrite(true);

                for (int i = 0; i < KEYS; i++)
                    streamer.addData(i, i);
            }

            final long stopTime = U.currentTimeMillis() + TEST_TIME;

            final AtomicInteger nodeIdx = new AtomicInteger();

            IgniteInternalFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Ignite ignite = ignite(nodeIdx.getAndIncrement());

                    log.info("Check get [node=" + ignite.name() +
                        ", client=" + ignite.configuration().isClientMode() + ']');

                    IgniteCache<Object, Object> cache = ignite.cache(ccfg.getName());

                    while (U.currentTimeMillis() < stopTime)
                        checkGet(cache);

                    return null;
                }
            }, SRVS + CLIENTS, "get-thread");

            final AtomicInteger restartNodeIdx = new AtomicInteger(SRVS + CLIENTS);

            final AtomicBoolean clientNode = new AtomicBoolean();

            IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int nodeIdx = restartNodeIdx.getAndIncrement();

                    Thread.currentThread().setName("restart-thread-" + nodeIdx);

                    boolean clientMode = clientNode.compareAndSet(false, true);

                    while (U.currentTimeMillis() < stopTime) {
                        if (clientMode)
                            client.set(true);

                        log.info("Restart node [node=" + nodeIdx + ", client=" + clientMode + ']');

                        try {
                            Ignite ignite = startGrid(nodeIdx);

                            IgniteCache<Object, Object> cache;

                            if (clientMode && ccfg.getNearConfiguration() != null)
                                cache = ignite.createNearCache(ccfg.getName(), new NearCacheConfiguration<>());
                            else
                                cache = ignite.cache(ccfg.getName());

                            checkGet(cache);

                            IgniteInternalFuture<?> syncFut = ((IgniteCacheProxy)cache).context().preloader().syncFuture();

                            while (!syncFut.isDone() && U.currentTimeMillis() < stopTime)
                                checkGet(cache);

                            checkGet(cache);
                        }
                        finally {
                            stopGrid(nodeIdx);
                        }
                    }

                    return null;
                }
            }, restartCnt + 1, "restart-thread");

            fut1.get();
            fut2.get();
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param cache Cache.
     */
    private void checkGet(IgniteCache<Object, Object> cache) {
        for (int i = 0; i < KEYS; i++)
            assertEquals(i, cache.get(i));

        Set<Integer> keys = new HashSet<>();

        for (int i = 0; i < KEYS; i++) {
            keys.add(i);

            if (keys.size() == 100) {
                Map<Object, Object> vals = cache.getAll(keys);

                for (Object key : keys)
                    assertEquals(key, vals.get(key));

                keys.clear();
            }
        }
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param near If {@code true} near cache is enabled.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(CacheMode cacheMode, int backups, boolean near) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);

        if (cacheMode != REPLICATED)
            ccfg.setBackups(backups);

        if (near)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());

        ccfg.setRebalanceMode(ASYNC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }
}
