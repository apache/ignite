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

import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.inmemory.GridTestSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 */
public class IgniteCacheOffheapEvictQueryTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setSwapSpaceSpi(new GridTestSwapSpaceSpi());

        CacheConfiguration<?,?> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setBackups(0);
        cacheCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        cacheCfg.setEvictionPolicy(null);
        cacheCfg.setNearConfiguration(null);

        cacheCfg.setSqlOnheapRowCacheSize(128);

        cacheCfg.setIndexedTypes(
            Integer.class, Integer.class
        );

        cacheCfg.setOffHeapMaxMemory(2000); // Small offheap for evictions from offheap to swap.

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictAndRemove() throws Exception {
        final int KEYS_CNT = 3000;
        final int THREADS_CNT = 250;

        final IgniteCache<Integer,Integer> c = startGrid().cache(null);

        for (int i = 0; i < KEYS_CNT; i++) {
            c.put(i, i);

            if ((i & 1) == 0)
                c.localEvict(F.asList(i));
        }

        X.println("___ Cache loaded...");

        final CyclicBarrier b = new CyclicBarrier(THREADS_CNT, new Runnable() {
            @Override public void run() {
                X.println("___ go!");
            }
        });

        final AtomicInteger keys = new AtomicInteger(KEYS_CNT);

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                Random rnd = new GridRandom();

                try {
                    b.await();
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedException(e);
                }
                catch (BrokenBarrierException e) {
                    throw new IllegalStateException(e);
                }

                while (keys.get() > 0) {
                    int k = rnd.nextInt(KEYS_CNT);

                    try {
                        switch (rnd.nextInt(4)) {
                            case 0:
                                c.localEvict(F.asList(k));

                                break;

                            case 1:
                                c.get(k);

                                break;

                            case 2:
                                if (c.remove(k))
                                    keys.decrementAndGet();

                                break;

                            case 3:
                                c.query(new SqlFieldsQuery("select _val from Integer where _key between ? and ?")
                                    .setArgs(k, k + 20).setLocal(true)).getAll();

                                break;
                        }
                    }
                    catch (CacheException e) {
                        String msgStart = "Failed to get value for key:";

                        for (Throwable th = e; th != null; th = th.getCause()) {
                            String msg = th.getMessage();

                            if (msg != null && msg.startsWith(msgStart)) {
                                int dot = msg.indexOf('.', msgStart.length());

                                assertTrue(dot != -1);

                                final Integer failedKey = Integer.parseInt(msg.substring(msgStart.length(), dot).trim());

                                X.println("___ failed key: " + failedKey);

                                break;
                            }
                        }

                        LT.warn(log, e.getMessage());

                        return;
                    }
                }
            }
        }, THREADS_CNT);

        try {
            fut.get(60_000);

            if (c.size(CachePeekMode.ALL) != 0)
                fail("Not all keys removed.");

            X.println("___ all keys removed");
        }
        catch (IgniteFutureTimeoutCheckedException ignored) {
            X.println("___ timeout");
            X.println("___ keys: " + keys.get());

            keys.set(0);

            fut.get();
        }
    }
}
