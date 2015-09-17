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
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheAffEarlySelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static int GRID_CNT = 8;

    /** Operation timeout. */
    private static long OP_TIMEOUT = 5000;

    /** Always dump threads or only once per operation. */
    private static boolean ALWAYS_DUMP_THREADS = false;

    /** Stopped. */
    private volatile boolean stopped;

    /** Iteration. */
    private int iters = 10;

    /** Concurrent. */
    private boolean concurrent = true;

    /** Futs. */
    private Collection<IgniteInternalFuture<?>> futs = new ArrayList<>(GRID_CNT);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder(true);
        finder.setAddresses(Collections.singletonList("127.0.0.1:47500..47510"));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(finder);

        cfg.setDiscoverySpi(discoSpi);

        OptimizedMarshaller marsh = new OptimizedMarshaller();
        marsh.setRequireSerializable(false);

        cfg.setMarshaller(marsh);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 6 * 60 * 1000L;
    }

    /**
     *
     */
    public void testStartNodes() throws Exception {
        for (int i = 0; i < iters; i++) {
            try {
                System.out.println("*** Iteration " + (i + 1) + '/' + iters);

                IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                    @Override public void run() {
                        try {
                            doTest();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }, 1);

                fut.get(30000);
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                // No-op.
            }
            finally {
                stopAllGrids(true);
            }
        }
    }

    /**
     *
     */
    public void doTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            final int idx = i;

            final Ignite grid = concurrent ? null : startGrid(idx);

            IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    Random rnd = new Random();

                    try {
                        final Ignite ignite = grid == null ? startGrid(idx) : grid;

                        final IgniteCache<UUID, UUID> cache = getCache(ignite).withAsync();

                        CacheAffEarlySelfTest.this.execute(cache, new IgniteInClosure<IgniteCache<UUID,UUID>>() {
                            @Override public void apply(IgniteCache<UUID, UUID> entries) {
                                cache.put(ignite.cluster().localNode().id(), UUID.randomUUID());
                            }
                        });

                        while (!stopped) {
                            int val = Math.abs(rnd.nextInt(100));
                            if (val >= 0 && val < 40)
                                execute(cache, new IgniteInClosure<IgniteCache<UUID, UUID>>() {
                                    @Override public void apply(IgniteCache<UUID, UUID> entries) {
                                        cache.containsKey(ignite.cluster().localNode().id());
                                    }
                                });
                            else if (val >= 40 && val < 80)
                                execute(cache, new IgniteInClosure<IgniteCache<UUID, UUID>>() {
                                    @Override public void apply(IgniteCache<UUID, UUID> entries) {
                                        cache.get(ignite.cluster().localNode().id());
                                    }
                                });
                            else
                                execute(cache, new IgniteInClosure<IgniteCache<UUID, UUID>>() {
                                    @Override public void apply(IgniteCache<UUID, UUID> entries) {
                                        cache.put(ignite.cluster().localNode().id(), UUID.randomUUID());
                                    }
                                });

                            Thread.sleep(50);
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, 1);

            futs.add(fut);
        }

        Thread.sleep(10000);

        stopped = true;

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();
    }

    /**
     * @param cache Cache.
     * @param c Closure.
     */
    private void execute(IgniteCache<UUID, UUID> cache, IgniteInClosure<IgniteCache<UUID, UUID>> c) {
        c.apply(cache);

        IgniteFuture<Object> fut = cache.future();

        boolean success = false;

        int iter = 0;

        while (!success) {
            try {
                fut.get(OP_TIMEOUT);

                success = true;
            }
            catch (IgniteFutureTimeoutException e) {
                debug(iter == 0 || ALWAYS_DUMP_THREADS);
            }

            iter++;
        }
    }

    /**
     *
     */
    private void debug(boolean dumpThreads) {
        log.info("DUMPING DEBUG INFO:");

        for (Ignite ignite : G.allGrids())
            ((IgniteKernal)ignite).dumpDebugInfo();

        if (dumpThreads) {
            U.dumpThreads(null);

            U.dumpThreads(log);
        }
    }

    /**
     * @param grid Grid.
     */
    private IgniteCache<UUID, UUID> getCache(Ignite grid) {
        CacheConfiguration<UUID, UUID> ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setBackups(1);
        ccfg.setNearConfiguration(null);

        return grid.getOrCreateCache(ccfg);
    }
}
