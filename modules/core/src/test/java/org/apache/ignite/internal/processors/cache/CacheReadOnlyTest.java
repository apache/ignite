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
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class CacheReadOnlyTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 3;

    /** Replicated atomic cache. */
    private static final String REPL_ATOMIC_CACHE = "repl_atomic_cache";

    /** Replicated transactional cache. */
    private static final String REPL_TX_CACHE = "repl_tx_cache";

    /** Partitioned atomic cache. */
    private static final String PART_ATOMIC_CACHE = "part_atomic_cache";

    /** Partitioned transactional cache. */
    private static final String PART_TX_CACHE = "part_tx_cache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        changeCacheReadOnlyMode(REPL_ATOMIC_CACHE, false);
        changeCacheReadOnlyMode(REPL_TX_CACHE, false);
        changeCacheReadOnlyMode(PART_ATOMIC_CACHE, false);
        changeCacheReadOnlyMode(PART_TX_CACHE, false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(
            cacheConfiguration(REPL_ATOMIC_CACHE, REPLICATED, ATOMIC, null),
            cacheConfiguration(REPL_TX_CACHE, REPLICATED, TRANSACTIONAL, null),
            cacheConfiguration(PART_ATOMIC_CACHE, PARTITIONED, ATOMIC, "part_grp"),
            cacheConfiguration(PART_TX_CACHE, PARTITIONED, TRANSACTIONAL, "part_grp")
        );

        return cfg;
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param grpName Cache group name.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(String name, CacheMode cacheMode,
        CacheAtomicityMode atomicityMode, String grpName) {
        return new CacheConfiguration<Integer, Integer>()
            .setName(name)
            .setCacheMode(cacheMode)
            .setAtomicityMode(atomicityMode)
            .setGroupName(grpName);
    }

    /**
     *
     */
    public void testCachePutRemove() {
        assertCacheReadOnlyMode(PART_ATOMIC_CACHE, false);
        assertCacheReadOnlyMode(PART_TX_CACHE, false);
        assertCacheReadOnlyMode(REPL_ATOMIC_CACHE, false);
        assertCacheReadOnlyMode(REPL_TX_CACHE, false);

        changeCacheReadOnlyMode(REPL_ATOMIC_CACHE, true);

        assertCacheReadOnlyMode(PART_ATOMIC_CACHE, false);
        assertCacheReadOnlyMode(PART_TX_CACHE, false);
        assertCacheReadOnlyMode(REPL_ATOMIC_CACHE, true);
        assertCacheReadOnlyMode(REPL_TX_CACHE, false);

        changeCacheReadOnlyMode(REPL_ATOMIC_CACHE, false);
        // This will also change PART_TX_CACHE read-only mode, since caches are in the same cache group.
        changeCacheReadOnlyMode(PART_ATOMIC_CACHE, true);

        assertCacheReadOnlyMode(PART_ATOMIC_CACHE, true);
        assertCacheReadOnlyMode(PART_TX_CACHE, true);
        assertCacheReadOnlyMode(REPL_ATOMIC_CACHE, false);
        assertCacheReadOnlyMode(REPL_TX_CACHE, false);

        changeCacheReadOnlyMode(REPL_TX_CACHE, true);
        changeCacheReadOnlyMode(PART_ATOMIC_CACHE, false);

        assertCacheReadOnlyMode(PART_ATOMIC_CACHE, false);
        assertCacheReadOnlyMode(PART_TX_CACHE, false);
        assertCacheReadOnlyMode(REPL_ATOMIC_CACHE, false);
        assertCacheReadOnlyMode(REPL_TX_CACHE, true);

        changeCacheReadOnlyMode(REPL_TX_CACHE, false);

        assertCacheReadOnlyMode(PART_ATOMIC_CACHE, false);
        assertCacheReadOnlyMode(PART_TX_CACHE, false);
        assertCacheReadOnlyMode(REPL_ATOMIC_CACHE, false);
        assertCacheReadOnlyMode(REPL_TX_CACHE, false);
    }

    /**
     *
     */
    public void testDataStreamerReadOnly() {
        assertDataStreamerReadOnlyMode(PART_ATOMIC_CACHE, false);
        assertDataStreamerReadOnlyMode(PART_TX_CACHE, false);
        assertDataStreamerReadOnlyMode(REPL_ATOMIC_CACHE, false);
        assertDataStreamerReadOnlyMode(REPL_TX_CACHE, false);

        changeCacheReadOnlyMode(REPL_ATOMIC_CACHE, true);

        assertDataStreamerReadOnlyMode(PART_ATOMIC_CACHE, false);
        assertDataStreamerReadOnlyMode(PART_TX_CACHE, false);
        assertDataStreamerReadOnlyMode(REPL_ATOMIC_CACHE, true);
        assertDataStreamerReadOnlyMode(REPL_TX_CACHE, false);

        changeCacheReadOnlyMode(REPL_ATOMIC_CACHE, false);
        // This will also change PART_TX_CACHE read-only mode, since caches are in the same cache group.
        changeCacheReadOnlyMode(PART_ATOMIC_CACHE, true);

        assertDataStreamerReadOnlyMode(PART_ATOMIC_CACHE, true);
        assertDataStreamerReadOnlyMode(PART_TX_CACHE, true);
        assertDataStreamerReadOnlyMode(REPL_ATOMIC_CACHE, false);
        assertDataStreamerReadOnlyMode(REPL_TX_CACHE, false);

        changeCacheReadOnlyMode(REPL_TX_CACHE, true);
        changeCacheReadOnlyMode(PART_ATOMIC_CACHE, false);

        assertDataStreamerReadOnlyMode(PART_ATOMIC_CACHE, false);
        assertDataStreamerReadOnlyMode(PART_TX_CACHE, false);
        assertDataStreamerReadOnlyMode(REPL_ATOMIC_CACHE, false);
        assertDataStreamerReadOnlyMode(REPL_TX_CACHE, true);

        changeCacheReadOnlyMode(REPL_TX_CACHE, false);

        assertDataStreamerReadOnlyMode(PART_ATOMIC_CACHE, false);
        assertDataStreamerReadOnlyMode(PART_TX_CACHE, false);
        assertDataStreamerReadOnlyMode(REPL_ATOMIC_CACHE, false);
        assertDataStreamerReadOnlyMode(REPL_TX_CACHE, false);
    }

    /**
     * Change cache read only mode on all nodes.
     *
     * @param cacheName Cache name.
     * @param readOnly Read only.
     */
    private void changeCacheReadOnlyMode(String cacheName, boolean readOnly) {
        for (int idx = 0; idx < SRVS; idx++) {
            IgniteEx ignite = grid(idx);

            ignite.context().cache().cache(cacheName).context().group().readOnly(readOnly);
        }
    }

    /**
     * Asserts that cache in read-only or in read/write mode on all nodes.
     *
     * @param cacheName Cache name.
     * @param readOnly If {@code true} then cache must be in read only mode, else in read/write mode.
     */
    private void assertCacheReadOnlyMode(String cacheName, boolean readOnly) {
        Random rnd = new Random();

        for (Ignite ignite : G.allGrids()) {
            IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

            for (int i = 0; i < 10; i++) {
                cache.get(rnd.nextInt(100)); // All gets must succeed.

                if (readOnly) {
                    // All puts must fail.
                    try {
                        cache.put(rnd.nextInt(100), rnd.nextInt());

                        fail("Put must fail for cache " + cacheName);
                    }
                    catch (Exception e) {
                        // No-op.
                    }

                    // All removes must fail.
                    try {
                        cache.remove(rnd.nextInt(100));

                        fail("Remove must fail for cache " + cacheName);
                    }
                    catch (Exception e) {
                        // No-op.
                    }
                }
                else {
                    cache.put(rnd.nextInt(100), rnd.nextInt()); // All puts must succeed.

                    cache.remove(rnd.nextInt(100)); // All removes must succeed.
                }
            }
        }
    }

    /**
     * @param cacheName Cache name.
     * @param readOnly If {@code true} then data streamer must fail, else succeed.
     */
    private void assertDataStreamerReadOnlyMode(String cacheName, boolean readOnly) {
        Random rnd = new Random();

        for (Ignite ignite : G.allGrids()) {
            boolean failed = false;

            try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(cacheName)) {
                for (int i = 0; i < 10; i++)
                    streamer.addData(rnd.nextInt(1000), rnd.nextInt());
            }
            catch (CacheException ignored) {
                failed = true;
            }

            if (failed != readOnly)
                fail("Streaming to " + cacheName + " must " + (readOnly ? "fail" : "succeed"));
        }
    }
}
