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

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.List;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Test different cache modes for query entry
 */
public class IgniteSqlEntryCacheModeAgnosticTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Host. */
    public static final String HOST = "127.0.0.1";

    /** Partitioned cache name. */
    private static final String PARTITIONED_CACHE_NAME = "PART_CACHE";

    /** Replicated cache name. */
    private static final String REPLICATED_CACHE_NAME = "REPL_CACHE";

    /** Local cache name. */
    private static final String LOCAL_CACHE_NAME = "LOCAL_CACHE";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setLocalHost(HOST);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        c.setCacheConfiguration(cacheConfiguration(LOCAL_CACHE_NAME),
            cacheConfiguration(REPLICATED_CACHE_NAME), cacheConfiguration(PARTITIONED_CACHE_NAME));

        return c;
    }

    /**
     * Provides configuration for cache given its name
     * @param cacheName Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfiguration(String cacheName) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setAffinity(new RendezvousAffinityFunction());

        cfg.setAtomicityMode(TRANSACTIONAL);

        switch (cacheName) {
            case LOCAL_CACHE_NAME:
                cfg.setCacheMode(LOCAL);
                break;
            case REPLICATED_CACHE_NAME:
                cfg.setCacheMode(REPLICATED);
                break;
            case PARTITIONED_CACHE_NAME:
                cfg.setCacheMode(PARTITIONED);
                cfg.setBackups(0);
                break;
            default:
                throw new Exception("Invalid cache name " + cacheName);
        }

        cfg.setName(cacheName);

        cfg.setIndexedTypes(Integer.class, String.class);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * It should not matter what cache mode does entry cache use, if there is no join
     */
    public void testCrossCacheModeQuery() throws Exception {
        Ignite ignite = startGrid();

        ignite.cache(LOCAL_CACHE_NAME).put(1, LOCAL_CACHE_NAME);
        ignite.cache(REPLICATED_CACHE_NAME).put(1, REPLICATED_CACHE_NAME);
        ignite.cache(PARTITIONED_CACHE_NAME).put(1, PARTITIONED_CACHE_NAME);

        final List<String> cacheNamesList = F.asList(LOCAL_CACHE_NAME, REPLICATED_CACHE_NAME, PARTITIONED_CACHE_NAME);

        for(String entryCacheName: cacheNamesList) {
            for(String qryCacheName: cacheNamesList) {
                if (entryCacheName.equals(qryCacheName))
                    continue;

                QueryCursor<List<?>> cursor = ignite.cache(entryCacheName).query(
                    new SqlFieldsQuery("SELECT _VAL FROM \"" + qryCacheName + "\".String"));

                assertEquals(qryCacheName, (String)cursor.getAll().get(0).get(0));
            }
        }
    }
}
