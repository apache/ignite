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

package org.apache.ignite.examples;

import java.util.Collections;
import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 *
 */
@SuppressWarnings({"CommentAbsent", "unchecked", "PointlessBooleanExpression"})
public class ScanQueryStuff2 {
//    private static final boolean CHECK1 = true;
        private static final boolean CHECK1 = false;

//    private static final boolean once = true;
    private static final boolean once = false;

    private static final int CNT;

    static {
        CNT = once ? 1 : 100_000;

        U.debugEnabled = once && CNT < 5;
    }

    /** */
    private static final int MAX_COUNT = 6_000_000;

    // DO NOT CHANGE IT!
    private static CacheMemoryMode memMode = CacheMemoryMode.ONHEAP_TIERED;

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start(igniteCfg())) {
            CacheConfiguration cfg = cacheCfg();

            IgniteCache cache = ignite.getOrCreateCache(cfg);

            for (int i = 0; i < CNT; i++)
                cache.put(i, i);

            System.out.println("Preloaded");

            int cnt = 0;
            long start = System.currentTimeMillis();

            boolean go = true;

            IgniteEx igniteEx = (IgniteEx)ignite;

            IgniteInternalCache<Object, Object> internalCache = igniteEx.context().cache().cache(null);

            GridDhtCacheAdapter<Object, Object> dht = internalCache.context().dht();

            GridCacheQueryManager<Object, Object> queries = internalCache.context().queries();

            int times = MAX_COUNT / CNT;

            while (go) {
                if (once)
                    go = false;

                if (CHECK1)
                    check1(dht);
                else
                    check2(queries);

                cnt++;

                if (cnt == times) {
                    cnt = 0;

                    System.out.println(">>>>> Takes " + (System.currentTimeMillis() - start) + " ms to process " + CNT + " items " + times + " times." );

                    start = System.currentTimeMillis();
                }
            }
        }
    }

    private static void check1(Object dht) {
        final Iterator<Cache.Entry> iterator = ((GridDhtCacheAdapter)dht).localEntriesIterator(true, false);

        int cnt = 0;

        while (iterator.hasNext()) {
            Cache.Entry next = iterator.next();

            next.getKey();
            next.getValue();

            cnt++;
        }

        if (cnt < CNT)
            throw new IllegalStateException("cnt:" + cnt);
    }

    private static void check2(GridCacheQueryManager<Object, Object> queryManager) {
//        Iterator<IgniteBiTuple<Object, Object>> iter = queryManager.scanHeapIterator();
//
//        int cnt = 0;
//
//        while (iter.hasNext()) {
//            IgniteBiTuple<Object, Object> next = iter.next();
//
//            next.getKey();
//            next.getValue();
//
//            cnt++;
//        }
//
////        if (entry == null)
////            throw new RuntimeException("unexcpeted");
//
//        if (cnt < CNT)
//            throw new IllegalStateException("cnt:" + cnt);
    }

    private static CacheConfiguration cacheCfg() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setMemoryMode(memMode);

        return cfg;
    }

    private static IgniteConfiguration igniteCfg() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singleton("127.0.0.1:47500..47509"));

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }
}
