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

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheClientsConcurrentStartTest extends GridCommonAbstractTest {
    /** */
    private static final int SRV_CNT = 4;

    /** */
    private static final int CLIENTS_CNT = 16;

    /** */
    private static final int CACHES = 30;

    /** Stopped. */
    private volatile boolean stopped;

    /** Iteration. */
    private static final int ITERATIONS = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi testSpi = new TcpDiscoverySpi() {
            @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg, long timeout) throws IOException, IgniteCheckedException {
                if (msg instanceof TcpDiscoveryCustomEventMessage && msg.verified()) {
                    try {
                        System.out.println(Thread.currentThread().getName() + " delay custom message");

                        U.sleep(ThreadLocalRandom.current().nextLong(500) + 100);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                super.writeToSocket(sock, out, msg, timeout);
            }
        };

        cfg.setMarshaller(null);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (getTestIgniteInstanceIndex(gridName) < SRV_CNT) {
            CacheConfiguration ccfgs[] = new CacheConfiguration[CACHES / 2];

            for (int i = 0; i < ccfgs.length; i++)
                ccfgs[i] = cacheConfiguration("cache-" + i);

            cfg.setCacheConfiguration(ccfgs);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000L;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartNodes() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            try {
                log.info("Iteration: " + (i + 1) + '/' + ITERATIONS);

                doTest();
            }
            finally {
                stopAllGrids(true);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest() throws Exception {
        final AtomicBoolean failed = new AtomicBoolean();

        startGrids(SRV_CNT);

        for (int i = 0; i < SRV_CNT; i++) {
            ((TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi()).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridDhtPartitionsFullMessage) {
                        try {
                            U.sleep(ThreadLocalRandom.current().nextLong(500) + 100);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    return false;
                }
            });
        }

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < CLIENTS_CNT; i++) {
            final int idx = i;

            IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    Random rnd = new Random();

                    try {
                        Ignite ignite = startClientGrid(SRV_CNT + idx);

                        assertTrue(ignite.configuration().isClientMode());

                        for (int i = 0; i < CACHES / 2; i++) {
                            String cacheName = "cache-" + rnd.nextInt(CACHES);

                            IgniteCache<Object, Object> cache = getCache(ignite, cacheName);

                            cache.put(ignite.cluster().localNode().id(), UUID.randomUUID());

                            IgniteAtomicSequence seq = ignite.atomicSequence("seq-" + rnd.nextInt(20), 0, true);

                            seq.getAndIncrement();
                        }

                        while (!stopped) {
                            IgniteCache<Object, Object> cache = getCache(ignite, "cache-" + rnd.nextInt(CACHES));

                            int val = Math.abs(rnd.nextInt(100));

                            if (val >= 0 && val < 40)
                                cache.containsKey(ignite.cluster().localNode().id());
                            else if (val >= 40 && val < 80)
                                cache.get(ignite.cluster().localNode().id());
                            else
                                cache.put(ignite.cluster().localNode().id(), UUID.randomUUID());

                            Thread.sleep(10);
                        }
                    }
                    catch (Exception e) {
                        log.error("Unexpected error: " + e, e);

                        failed.set(true);
                    }
                }
            }, 1, "client-thread");

            futs.add(fut);
        }

        Thread.sleep(10_000);

        stopped = true;

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();

        assertFalse(failed.get());
    }

    /**
     * @param grid Grid.
     * @return Cache.
     */
    private IgniteCache getCache(Ignite grid, String cacheName) {
        return grid.getOrCreateCache(cacheConfiguration(cacheName));
    }

    private CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(cacheName);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(2);
        ccfg.setNearConfiguration(null);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        return ccfg;
    }
}
