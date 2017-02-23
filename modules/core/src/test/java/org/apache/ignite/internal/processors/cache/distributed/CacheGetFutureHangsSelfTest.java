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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Test for reproducing problems during simultaneously Ignite instances stopping and cache requests executing.
 */
public class CacheGetFutureHangsSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    private static final int GRID_CNT = 8;

    /** */
    private AtomicReferenceArray<Ignite> nodes;

    /** */
    private volatile boolean done;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        OptimizedMarshaller marsh = new OptimizedMarshaller();
        marsh.setRequireSerializable(false);

        cfg.setMarshaller(marsh);

        CacheConfiguration ccfg = defaultCacheConfiguration();
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setWriteSynchronizationMode(PRIMARY_SYNC);
        ccfg.setNearConfiguration(null);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testContainsKeyFailover() throws Exception {
        int cnt = 3;

        for (int i = 0; i < cnt; i++) {
            try {
                log.info("Iteration: " + (i + 1) + '/' + cnt);

                doTestFailover();
            }
            finally {
                stopAllGrids();
            }
        }
    }

    /**
     * Executes one test iteration.
     *
     * @throws Exception If failed.
     */
    private void doTestFailover() throws Exception {
        try {
            done = false;

            nodes = new AtomicReferenceArray<>(GRID_CNT);

            startGridsMultiThreaded(GRID_CNT, false);

            for (int i = 0; i < GRID_CNT ; i++)
                assertTrue(nodes.compareAndSet(i, null, ignite(i)));

            List<IgniteInternalFuture> futs = new ArrayList<>();

            for (int i = 0; i < GRID_CNT + 1; i++) {
                futs.add(multithreadedAsync(new Runnable() {
                    @Override public void run() {
                        T2<Ignite, Integer> ignite;

                        Set<Integer> keys = F.asSet(1, 2, 3, 4, 5);

                        while ((ignite = randomNode()) != null) {
                            IgniteCache<Object, Object> cache = ignite.get1().cache(null);

                            for (int i = 0; i < 100; i++)
                                cache.containsKey(ThreadLocalRandom.current().nextInt(100_000));

                            cache.containsKeys(keys);

                            assertTrue(nodes.compareAndSet(ignite.get2(), null, ignite.get1()));

                            try {
                                Thread.sleep(ThreadLocalRandom.current().nextLong(50));
                            }
                            catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }, 1, "containsKey-thread-" + i));

                futs.add(multithreadedAsync(new Runnable() {
                    @Override public void run() {
                        T2<Ignite, Integer> ignite;

                        while ((ignite = randomNode()) != null) {
                            IgniteCache<Object, Object> cache = ignite.get1().cache(null);

                            for (int i = 0; i < 100; i++)
                                cache.put(ThreadLocalRandom.current().nextInt(100_000), UUID.randomUUID());

                            assertTrue(nodes.compareAndSet(ignite.get2(), null, ignite.get1()));

                            try {
                                Thread.sleep(ThreadLocalRandom.current().nextLong(50));
                            }
                            catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }, 1, "put-thread-" + i));
            }

            try {
                int aliveGrids = GRID_CNT;

                while (aliveGrids > 0) {
                    T2<Ignite, Integer> ignite = randomNode();

                    assert ignite != null;

                    Ignite ignite0 = ignite.get1();

                    log.info("Stop node: " + ignite0.name());

                    ignite0.close();

                    log.info("Node stop finished: " + ignite0.name());

                    aliveGrids--;
                }
            }
            finally {
                done = true;
            }

            for (IgniteInternalFuture fut : futs)
                fut.get();
        }
        finally {
            done = true;
        }
    }

    /**
     * @return Random node and its index.
     */
    @Nullable private T2<Ignite, Integer> randomNode() {
        while (!done) {
            int idx = ThreadLocalRandom.current().nextInt(GRID_CNT);

            Ignite ignite = nodes.get(idx);

            if (ignite != null && nodes.compareAndSet(idx, ignite, null))
                return new T2<>(ignite, idx);
        }

        return null;
    }
}
