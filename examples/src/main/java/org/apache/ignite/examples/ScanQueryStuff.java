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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 *
 */
public class ScanQueryStuff {
    public static final int CNT = 1_500_000;

    public static void main(String[] args) {
        Ignite ignite = Ignition.start(igniteCfg());

        CacheConfiguration cfg = cacheCfg();

        IgniteCache cache = ignite.getOrCreateCache(cfg);

        for (int i = 0; i < CNT; i++)
            cache.put(i, i);

        System.out.println("Preloaded");

        long start = System.currentTimeMillis();

        while (true) {
            Iterable iterable = cache.localEntries(CachePeekMode.PRIMARY, CachePeekMode.OFFHEAP, CachePeekMode.SWAP);

            for (Object ob : iterable) {
                if (ob == null)
                    throw new RuntimeException("unexpected");
            }

            System.out.println(">>>>> Takes " + (System.currentTimeMillis() - start) + " ms to process 1000 times." );

            start = System.currentTimeMillis();
        }
    }

    public static void _main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start(igniteCfg())) {
            CacheConfiguration cfg = cacheCfg();

            IgniteCache cache = ignite.getOrCreateCache(cfg);

            for (int i = 0; i < CNT; i++)
                cache.put(i, i);

            iterateOverLocalPartitions(ignite, cache);
//        iterateOverPartitionsUsingPublicPool(ignite, cache);
        }

        System.out.println("Finished");
    }

    private static CacheConfiguration cacheCfg() {
        CacheConfiguration cfg = new CacheConfiguration("testCache");

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
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

    private static void iterateOverLocalPartitions(Ignite ignite, IgniteCache cache) throws InterruptedException {
        System.out.println("Data is preloaded (iterateOverLocalPartitions)");

        int cnt = 0;
        long start = System.currentTimeMillis();

        while (true) {
            ScanQuery query = new ScanQuery();

            query.setLocal(true);

            QueryCursor cursor = cache.query(query);

            Iterator iter = cursor.iterator();

            Object entry = 0;

            while (iter.hasNext()) {
                entry = iter.next();

//                Thread.sleep(1, 0);
            }

            if (entry == null)
                throw new RuntimeException("unexcpeted");

            cnt++;

            if (cnt == 1) {
                cnt = 0;

                System.out.println(">>>>> Takes " + (System.currentTimeMillis() - start) + " ms to process 1000 times." );

                start = System.currentTimeMillis();
            }
        }
    }
}
