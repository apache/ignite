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

package org.apache.ignite.internal.processors.query;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests {@link IgniteH2Indexing} support {@link CacheConfiguration#setSqlSchema(String)} configuration.
 */
public class IgniteSqlSchemaIndexingTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     */
    private static CacheConfiguration cacheConfig(String name, boolean partitioned, Class<?>... idxTypes) {
        return new CacheConfiguration()
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setIndexedTypes(idxTypes);
    }

    /**
     * Tests collision for case insensitive sqlScheme.
     *
     * @throws Exception If failed.
     */
    public void testCaseSensitive() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                final CacheConfiguration cfg = cacheConfig("InSensitiveCache", true, Integer.class, Integer.class)
                    .setSqlSchema("InsensitiveCache");
                final CacheConfiguration collisionCfg = cacheConfig("InsensitiveCache", true, Integer.class, Integer.class)
                    .setSqlSchema("Insensitivecache");
                IgniteConfiguration icfg = new IgniteConfiguration()
                    .setLocalHost("127.0.0.1")
                    .setCacheConfiguration(cfg, collisionCfg);

                Ignition.start(icfg);

                return null;
            }
        }, IgniteException.class, "insensitive schema name already registered");
    }

    public void testCacheUnregistration() throws Exception {
        try {
            startGridsMultiThreaded(3, true);

            final CacheConfiguration cfg = cacheConfig("InSensitiveCache", true, Integer.class, Integer.class)
                .setSqlSchema("InsensitiveCache");
            final CacheConfiguration collisionCfg = cacheConfig("InsensitiveCache", true, Integer.class, Integer.class)
                .setSqlSchema("InsensitiveCache");

            IgniteCache ic = ignite(0).createCache(cfg);

            ignite(0).destroyCache(ic.getName());

            ignite(0).createCache(collisionCfg); // Previous collision should be removed by now.

        }
        finally {
            stopAllGrids();
        }
    }

    // TODO add tests with dynamic cache unregistration, after resolve of https://issues.apache.org/jira/browse/IGNITE-1094
}
