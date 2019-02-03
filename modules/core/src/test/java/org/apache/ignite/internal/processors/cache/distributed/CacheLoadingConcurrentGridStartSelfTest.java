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

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests for cache data loading during simultaneous grids start.
 */
public class CacheLoadingConcurrentGridStartSelfTest extends GridCommonAbstractTest implements Serializable {
    /** Grids count */
    private static int GRIDS_CNT = 5;

    /** Keys count */
    private static int KEYS_CNT = 1_000_000;

    /** Client. */
    private volatile boolean client;

    /** Config. */
    private volatile boolean configured;

    /** Allow override. */
    protected volatile boolean allowOverwrite;

    /** Restarts. */
    protected volatile boolean restarts;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode());

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setBackups(1);

        ccfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(new TestCacheStoreAdapter()));

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 64));

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName)) {
            if (client)
                cfg.setClientMode(true);

            if (configured)
                cfg.setCacheConfiguration(ccfg);
        }
        else
            cfg.setCacheConfiguration(ccfg);

        if (!configured) {
            ccfg.setNodeFilter(new P1<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    String name = node.attribute(ATTR_IGNITE_INSTANCE_NAME).toString();

                    return !getTestIgniteInstanceName(0).equals(name);
                }
            });
        }

        return cfg;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testLoadCacheWithDataStreamer() throws Exception {
        configured = true;

        try {
            IgniteInClosure<Ignite> f = new IgniteInClosure<Ignite>() {
                @Override public void apply(Ignite grid) {
                    try (IgniteDataStreamer<Integer, String> dataStreamer = grid.dataStreamer(DEFAULT_CACHE_NAME)) {
                        dataStreamer.allowOverwrite(allowOverwrite);

                        for (int i = 0; i < KEYS_CNT; i++)
                            dataStreamer.addData(i, Integer.toString(i));
                    }

                    log.info("Data loaded.");
                }
            };

            loadCache(f);
        }
        finally {
            configured = false;
        }
    }

    /**
     * @throws Exception if failed
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-4210")
    @Test
    public void testLoadCacheFromStore() throws Exception {
        loadCache(new IgniteInClosure<Ignite>() {
            @Override public void apply(Ignite grid) {
                grid.cache(DEFAULT_CACHE_NAME).loadCache(null);
            }
        });
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testLoadCacheWithDataStreamerSequentialClient() throws Exception {
        client = true;

        try {
            loadCacheWithDataStreamerSequential();
        }
        finally {
            client = false;
        }
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testLoadCacheWithDataStreamerSequentialClientWithConfig() throws Exception {
        client = true;
        configured = true;

        try {
            loadCacheWithDataStreamerSequential();
        }
        finally {
            client = false;
            configured = false;
        }
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testLoadCacheWithDataStreamerSequential() throws Exception {
        loadCacheWithDataStreamerSequential();
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testLoadCacheWithDataStreamerSequentialWithConfigAndRestarts() throws Exception {
        restarts = true;
        configured = true;

        try {
            loadCacheWithDataStreamerSequential();
        }
        finally {
            restarts = false;
            configured = false;
        }
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testLoadCacheWithDataStreamerSequentialWithConfig() throws Exception {
        configured = true;

        try {
            loadCacheWithDataStreamerSequential();
        }
        finally {
            configured = false;
        }
    }

    /**
     * @throws Exception if failed
     */
    private void loadCacheWithDataStreamerSequential() throws Exception {
        startGrid(1);

        Ignite g0 = startGrid(0);

        IgniteInternalFuture<Object> restartFut = runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (restarts) {
                    stopGrid(1);

                    startGrid(1);

                    U.sleep(100);
                }

                return null;
            }
        });

        CountDownLatch startNodesLatch = new CountDownLatch(1);
        IgniteInternalFuture<Object> fut = runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                startNodesLatch.await();

                for (int i = 2; i < GRIDS_CNT; i++)
                    startGrid(i);

                return null;
            }
        });

        final HashSet<IgniteFuture> set = new HashSet<>();

        boolean stop = false;
        int insertedKeys = 0;

        startNodesLatch.countDown();

        try (IgniteDataStreamer<Integer, String> dataStreamer = g0.dataStreamer(DEFAULT_CACHE_NAME)) {
            dataStreamer.allowOverwrite(allowOverwrite);
            ((DataStreamerImpl)dataStreamer).maxRemapCount(Integer.MAX_VALUE);

            long startingEndTs = -1L;

            while (!stop) {
                set.add(dataStreamer.addData(insertedKeys, "Data"));
                insertedKeys = insertedKeys + 1;

                if (insertedKeys % 100000 == 0)
                    log.info("Streaming " + insertedKeys + "'th entry.");

                //When all nodes started we continue restart nodes during 1 second and stop it after this timeout.
                if (fut.isDone() && startingEndTs == -1)
                    startingEndTs = System.currentTimeMillis();

                if (startingEndTs != -1) //Nodes starting was ended and we check restarts duration after it.
                    restarts = (System.currentTimeMillis() - startingEndTs) < 1000;

                //Stop test when all keys were inserted or restarts timeout was exceeded.
                stop = insertedKeys >= KEYS_CNT || (fut.isDone() && !restarts);
            }
        }

        log.info("Data loaded.");

        restarts = false;

        fut.get();
        restartFut.get();

        for (IgniteFuture res : set)
            assertNull(res.get());

        IgniteCache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        long size = cache.size(CachePeekMode.PRIMARY);

        if (size != insertedKeys) {
            Set<Integer> failedKeys = new LinkedHashSet<>();

            for (int i = 0; i < insertedKeys; i++)
                if (!cache.containsKey(i)) {
                    log.info("Actual cache size: " + size);

                    for (Ignite ignite : G.allGrids()) {
                        IgniteEx igniteEx = (IgniteEx)ignite;

                        log.info("Missed key info:" +
                            igniteEx.localNode().id() +
                            " primary=" +
                            ignite.affinity(DEFAULT_CACHE_NAME).isPrimary(igniteEx.localNode(), i) +
                            " backup=" +
                            ignite.affinity(DEFAULT_CACHE_NAME).isBackup(igniteEx.localNode(), i) +
                            " local peek=" +
                            ignite.cache(DEFAULT_CACHE_NAME).localPeek(i, CachePeekMode.ONHEAP));
                    }

                    for (int j = i; j < i + 10000; j++) {
                        if (!cache.containsKey(j))
                            failedKeys.add(j);
                    }

                    break;
                }

            assert failedKeys.isEmpty() : "Some failed keys: " + failedKeys.toString();
        }

        assertCacheSize(insertedKeys);
    }

    /**
     * Loads cache using closure and asserts cache size.
     *
     * @param f cache loading closure
     * @throws Exception if failed
     */
    protected void loadCache(IgniteInClosure<Ignite> f) throws Exception {
        Ignite g0 = startGrid(0);

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Ignite>() {
            @Override public Ignite call() throws Exception {
                return startGridsMultiThreaded(1, GRIDS_CNT - 1);
            }
        });

        try {
            f.apply(g0);
        }
        finally {
            fut.get();
        }

        assertCacheSize(KEYS_CNT);
    }

    /**
     * @throws Exception If failed.
     */
    private void assertCacheSize(int expectedCacheSize) throws Exception {
        final IgniteCache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        boolean consistentCache = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                int size = cache.size(CachePeekMode.PRIMARY);

                if (size != expectedCacheSize)
                    log.info("Cache size: " + size);

                int total = 0;

                for (int i = 0; i < GRIDS_CNT; i++)
                    total += grid(i).cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY);

                if (total != expectedCacheSize)
                    log.info("Total size: " + size);

                return size == expectedCacheSize && expectedCacheSize == total;
            }
        }, 2 * 60_000);

        assertTrue("Data lost. Actual cache size: " + cache.size(CachePeekMode.PRIMARY), consistentCache);
    }

    /**
     * Cache store adapter.
     */
    private static class TestCacheStoreAdapter extends CacheStoreAdapter<Integer, String> implements Serializable {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, String> f, Object... args) {
            for (int i = 0; i < KEYS_CNT; i++)
                f.apply(i, Integer.toString(i));
        }

        /** {@inheritDoc} */
        @Nullable @Override public String load(Integer i) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends String> entry)
            throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object o) throws CacheWriterException {
            // No-op.
        }
    }
}
