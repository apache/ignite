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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.swapspace.inmemory.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;

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

    /**
     * @throws Exception If failed.
     */
    public void testEvictAndRemove() throws Exception {
        final int KEYS_CNT = 3000;
        final int THREADS_CNT = 50;

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

                        LT.warn(log, null, e.getMessage());

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
        catch (IgniteFutureTimeoutCheckedException e) {
            X.println("___ timeout");
            X.println("___ keys: " + keys.get());

            keys.set(0);

            fut.get();
        }
    }
}
