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
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class ClusterReadOnlyModeTest extends GridCommonAbstractTest {
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

        changeClusterReadOnlyMode(false);
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
        assertCachesReadOnlyMode(false);

        changeClusterReadOnlyMode(true);

        assertCachesReadOnlyMode(true);

        changeClusterReadOnlyMode(false);

        assertCachesReadOnlyMode(false);
    }

    /**
     *
     */
    public void testDataStreamerReadOnly() {
        assertDataStreamerReadOnlyMode(false);

        changeClusterReadOnlyMode(true);

        assertDataStreamerReadOnlyMode(true);

        changeClusterReadOnlyMode(false);

        assertDataStreamerReadOnlyMode(false);
    }

    /**
     * Change read only mode on all nodes.
     *
     * @param readOnly Read only.
     */
    private void changeClusterReadOnlyMode(boolean readOnly) {
        for (int idx = 0; idx < SRVS; idx++) {
            IgniteEx ignite = grid(idx);

            ignite.context().cache().context().readOnlyMode(readOnly);
        }
    }

    /**
     * Asserts that all caches in read-only or in read/write mode on all nodes.
     *
     * @param readOnly If {@code true} then cache must be in read only mode, else in read/write mode.
     */
    private void assertCachesReadOnlyMode(boolean readOnly) {
        Random rnd = new Random();

        for (Ignite ignite : G.allGrids()) {
            for(String cacheName : F.asList(REPL_ATOMIC_CACHE, REPL_TX_CACHE, PART_ATOMIC_CACHE, PART_TX_CACHE)) {
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
    }

    /**
     * @param readOnly If {@code true} then data streamer must fail, else succeed.
     */
    private void assertDataStreamerReadOnlyMode(boolean readOnly) {
        Random rnd = new Random();

        for (Ignite ignite : G.allGrids()) {
            for(String cacheName : F.asList(REPL_ATOMIC_CACHE, REPL_TX_CACHE, PART_ATOMIC_CACHE, PART_TX_CACHE)) {
                boolean failed = false;

                try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(cacheName)) {
                    for (int i = 0; i < 10; i++) {
                        ((DataStreamerImpl)streamer).maxRemapCount(5);

                        streamer.addData(rnd.nextInt(1000), rnd.nextInt());
                    }
                }
                catch (CacheException ignored) {
                    failed = true;
                }

                if (failed != readOnly)
                    fail("Streaming to " + cacheName + " must " + (readOnly ? "fail" : "succeed"));
            }
        }
    }
}
