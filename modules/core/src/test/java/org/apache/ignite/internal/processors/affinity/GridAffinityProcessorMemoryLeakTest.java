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

package org.apache.ignite.internal.processors.affinity;

import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Tests for {@link GridAffinityProcessor}.
 */
@GridCommonTest(group = "Affinity Processor")
public class GridAffinityProcessorMemoryLeakTest extends GridCommonAbstractTest {
    /** Max value for affinity history size name. Should be the same as in GridAffinityAssignmentCache.MAX_HIST_SIZE */
    private final int MAX_HIST_SIZE = getInteger(IGNITE_AFFINITY_HISTORY_SIZE, 100);

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setForceServerMode(true);

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(CACHE_NAME);

        cacheCfg.setStoreKeepBinary(true);

        cacheCfg.setCacheMode(CacheMode.LOCAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Test affinity functions caching and clean up.
     *
     * @throws Exception In case of any exception.
     */
    public void testAffinityProcessor() throws Exception {
        Ignite ignite = startGrid(0);

        IgniteKernal grid = (IgniteKernal)grid(0);

        IgniteCache<String, String> cache;

        IgniteCache<String, String> globalCache = getOrCreateGlobalCache(ignite);

        IgniteDataStreamer<String, String> globalStreamer;

        int count = MAX_HIST_SIZE * 4;

        int size;

        do {
            try {
                cache = createLocalCache(ignite, count);

                cache.put("Key" + count, "Value" + count);

                cache.destroy();

                globalStreamer = createGlobalStreamer(ignite, globalCache);

                globalStreamer.addData("GlobalKey" + count, "GlobalValue" + count);

                globalStreamer.flush();

                globalStreamer.close();

                size = ((ConcurrentSkipListMap)GridTestUtils.getFieldValue(grid.context().affinity(), "affMap")).size();

                assertTrue("Cache has size that bigger then expected [size=" + size + "" +
                    ", expLimit=" + MAX_HIST_SIZE * 3 + "]", size < MAX_HIST_SIZE * 3);
            }
            catch (Exception e) {
                fail("Error was handled [" + e.getMessage() + "]");
            }
        }
        while (count-- > 0);
    }

    /**
     * Creates global cache.
     *
     * @param ignite instance of {@code Ignite}.
     * @param id unique id for local cache.
     * @return local cache instance.
     */
    private static IgniteCache<String, String> createLocalCache(Ignite ignite, long id) {
        final String cacheName = "localCache" + id;

        final CacheConfiguration<String, String> cCfg = new CacheConfiguration<>();

        cCfg.setName(cacheName);

        cCfg.setCacheMode(CacheMode.LOCAL);

        cCfg.setGroupName("some group");

        ignite.destroyCache(cacheName); // Local cache is not really local - reference can be kept by other nodes if restart during the load happens.

        return ignite.createCache(cCfg).withKeepBinary();
    }

    /**
     * Gets or creates global cache.
     *
     * @param ignite instance of {@code Ignite}.
     * @return global cache instance.
     */
    private static IgniteCache<String, String> getOrCreateGlobalCache(Ignite ignite) {
        final String cacheName = "GlobalCache";

        final CacheConfiguration<String, String> cCfg = new CacheConfiguration<>();

        cCfg.setName(cacheName);

        cCfg.setStoreKeepBinary(true);

        cCfg.setCacheMode(CacheMode.PARTITIONED);

        cCfg.setOnheapCacheEnabled(false);

        cCfg.setCopyOnRead(false);

        cCfg.setBackups(0);

        cCfg.setWriteBehindEnabled(false);

        cCfg.setReadThrough(false);

        return ignite.getOrCreateCache(cCfg).withKeepBinary();
    }

    /**
     * Creates streamer for global cache.
     *
     * @param ignite instance of {@code Ignite}.
     * @param cache instance of global cache.
     * @return instance of {@code IgniteDataStreamer}.
     */
    private static IgniteDataStreamer<String, String> createGlobalStreamer(Ignite ignite,
        IgniteCache<String, String> cache) {
        IgniteDataStreamer<String, String> streamer = ignite.dataStreamer(cache.getName());

        streamer.allowOverwrite(true);

        streamer.skipStore(true);

        streamer.keepBinary(false);

        return streamer;
    }
}
