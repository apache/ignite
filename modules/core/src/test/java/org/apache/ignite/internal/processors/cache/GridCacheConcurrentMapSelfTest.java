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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Grid cache concurrent hash map self test.
 */
public class GridCacheConcurrentMapSelfTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(LOCAL);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setStartSize(4);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cc);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRehash() throws Exception {
        IgniteCache<Integer, String> c = grid().cache(null);

        int cnt = 100 * 1024;

        for (int i = 0; i < cnt; i++) {
            c.put(i, Integer.toString(i));

            if (i > 0 && i % 50000 == 0)
                info(">>> " + i + " puts completed");
        }

        for (int i = 0; i < cnt; i++)
            assertEquals(Integer.toString(i), c.get(i));

        assertEquals(cnt, c.size());

        int idx = 0;

        for (Cache.Entry<Integer, String> e : c) {
            assertNotNull(e.getValue());

            idx++;
        }

        assertEquals(cnt, idx);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRehashRandom() throws Exception {
        IgniteCache<Integer, String> c = grid().cache(null);

        int cnt = 100 * 1024;

        Random rnd = new Random();

        Map<Integer, String> puts = new HashMap<>();

        for (int i = 0; i < cnt * 2; i++) {
            int key = rnd.nextInt(cnt);

            c.put(key, Integer.toString(key));

            puts.put(key, Integer.toString(key));

            if (i > 0 && i % 50000 == 0)
                info(">>> " + i + " puts completed");
        }

        for (Integer key : puts.keySet())
            assertEquals(Integer.toString(key), c.get(key));

        assertEquals(puts.size(), c.size());

        int idx = 0;

        for (Cache.Entry<Integer, String> e : c) {
            assertNotNull(e.getValue());

            idx++;
        }

        assertEquals(puts.size(), idx);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRehashMultithreaded1() throws Exception {
        final AtomicInteger tidGen = new AtomicInteger();

        final Random rand = new Random();

        final int cnt = 100 * 1024;

        multithreaded(new Callable<Object>() {
            @SuppressWarnings("UnusedAssignment")
            @Override public Object call() throws Exception {
                IgniteCache<Integer, String> c = grid().cache(null);

                int tid = tidGen.getAndIncrement();

                int start = 2 * 1024 * tid;

                Iterator<Cache.Entry<Integer, String>> it = null;

                boolean created = false;

                for (int i = start; i < start + cnt; i++) {
                    int key = i % cnt;

                    if (!created && i >= start + tid * 100) {
                        if (it == null)
                            it = c.iterator();

                        created = true;
                    }

                    c.put(key, Integer.toString(key));

                    c.get(rand.nextInt(cnt));
                }

                // Go through iterators.
                while(it.hasNext())
                    it.next();

                // Make sure that hard references are gone.
                it = null;

                for (int i = start; i < start + cnt; i++) {
                    int key = i % cnt;

                    assertEquals(Integer.toString(key), c.get(key));
                }

                assertEquals(cnt, c.size());

                int idx = 0;

                for (Cache.Entry<Integer, String> e : c) {
                    assertNotNull(e.getValue());

                    idx++;
                }

                assertEquals(cnt, idx);

                System.gc();

                return null;
            }
        }, 10);

        jcache().get(rand.nextInt(cnt));

        System.gc();

        Thread.sleep(1000);

        jcache().get(rand.nextInt(cnt));

        assertEquals(0, local().map.iteratorMapSize());
    }

    /**
     * @throws Exception If failed.
     */
    public void testRehashMultithreaded2() throws Exception {
        final AtomicInteger tidGen = new AtomicInteger(0);

        final Random rand = new Random();

        final int cnt = 100 * 1024;

        multithreaded(new Callable<Object>() {
            @SuppressWarnings("UnusedAssignment")
            @Override public Object call() throws Exception {
                IgniteCache<Integer, String> c = grid().cache(null);

                int tid = tidGen.getAndIncrement();

                int start = 2 * 1024 * tid;

                Iterator<Cache.Entry<Integer, String>> it1 = null;
                Iterator<Cache.Entry<Integer, String>> it2 = null;
                Iterator<Cache.Entry<Integer, String>> it3 = null;

                boolean forgot = false;

                for (int i = start; i < start + cnt; i++) {
                    int key = i % cnt;

                    if (!forgot && i >= start + tid * 100) {
                        if (it1 == null)
                            it1 = c.iterator();

                        if (it2 == null)
                            it2 = c.iterator();

                        if (it3 == null)
                            it3 = c.iterator();
                    }

                    c.put(key, Integer.toString(key));

                    c.get(rand.nextInt(cnt));

                    if (!forgot && i == cnt) {
                        info("Forgetting iterators [it1=" + it1 + ", it2=" + it2 + ", it3=" + it3 + ']');

                        // GC
                        it1 = null;
                        it2 = null;
                        it3 = null;

                        forgot = true;
                    }
                }

                // Make sure that hard references are gone.
                it1 = null;
                it2 = null;
                it3 = null;

                for (int i = start; i < start + cnt; i++) {
                    int key = i % cnt;

                    assertEquals(Integer.toString(key), c.get(key));
                }

                assertEquals(cnt, c.size());

                int idx = 0;

                for (Cache.Entry<Integer, String> e : c) {
                    assertNotNull(e.getValue());

                    idx++;
                }

                assertEquals(cnt, idx);

                System.gc();

                return null;
            }
        }, 10);

        jcache().get(rand.nextInt(cnt));

        System.gc();

        Thread.sleep(1000);

        jcache().get(rand.nextInt(cnt));

        assertEquals(0, local().map.iteratorMapSize());
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    public void testEmptyWeakIterator() throws Exception {
        final IgniteCache<Integer, String> c = grid().cache(null);

        for (int i = 0; i < 10; i++) {
            multithreaded(new Callable<Object>() {
                @SuppressWarnings("UnusedAssignment")
                @Override public Object call() throws Exception {
                    Iterator<Cache.Entry<Integer, String>> it = c.iterator();

                    for (int i = 0; i < 1000; i++) {
                        c.put(i, String.valueOf(i));

                        if (i == 0)
                            it.hasNext();
                    }

                    // Make sure that hard references are gone.
                    it = null;

                    System.gc();

                    return null;
                }
            }, Runtime.getRuntime().availableProcessors());

            for (int r = 0; r < 10; r++) {
                System.gc();

                c.get(100);

                if (local().map.iteratorMapSize() == 0)
                    break;
                else
                    U.sleep(500);
            }

            assertEquals(0, local().map.iteratorMapSize());
        }
    }
}