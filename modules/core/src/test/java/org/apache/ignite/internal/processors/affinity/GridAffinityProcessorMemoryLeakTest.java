/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_AFFINITY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Tests for {@link GridAffinityProcessor}.
 */
@GridCommonTest(group = "Affinity Processor")
@RunWith(JUnit4.class)
public class GridAffinityProcessorMemoryLeakTest extends GridCommonAbstractTest {
    /** Max value for affinity history size name. Should be the same as in GridAffinityAssignmentCache.MAX_HIST_SIZE */
    private final int MAX_HIST_SIZE = getInteger(IGNITE_AFFINITY_HISTORY_SIZE, 500);

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

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
    @Test
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
