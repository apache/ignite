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
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class IgniteDynamicCacheWithConfigStartSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "partitioned";

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        if (!client)
            cfg.setCacheConfiguration(cacheConfiguration(gridName));

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setIndexedTypes(String.class, String.class);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartCacheOnClient() throws Exception {
        int srvCnt = 3;

        startGrids(srvCnt);

        try {
            client = true;

            int clientCnt = 12;

            IgniteEx[] clients = new IgniteEx[clientCnt];

            for (int i = 0; i < clients.length; i++)
                clients[i] = startGrid(i + srvCnt);

            final AtomicInteger idx = new AtomicInteger();

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    final int idx0 = idx.getAndIncrement();

                    ignite(idx0).cache(CACHE_NAME).get(1);

                    return null;
                }
            }, clients.length, "runner");
        }
        finally {
            stopAllGrids();
        }
    }
}
