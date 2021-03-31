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

import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteClusterReadOnlyException;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.junit.Assert.fail;

/**
 * Utility class for testing grid read-only mode.
 */
public class ClusterReadOnlyModeTestUtils {
    /** Replicated atomic cache. */
    private static final String REPL_ATOMIC_CACHE = "repl_atomic_cache";

    /** Replicated transactional cache. */
    private static final String REPL_TX_CACHE = "repl_tx_cache";

    /** Replicated transactional cache. */
    private static final String REPL_MVCC_CACHE = "repl_mvcc_cache";

    /** Partitioned atomic cache. */
    public static final String PART_ATOMIC_CACHE = "part_atomic_cache";

    /** Partitioned transactional cache. */
    private static final String PART_TX_CACHE = "part_tx_cache";

    /** Partitioned mvcc transactional cache. */
    private static final String PART_MVCC_CACHE = "part_mvcc_cache";

    /**
     * @return Configured cache names.
     */
    public static Collection<String> cacheNames() {
        return F.asList(REPL_ATOMIC_CACHE, REPL_TX_CACHE, REPL_MVCC_CACHE, PART_ATOMIC_CACHE, PART_TX_CACHE, PART_MVCC_CACHE);
    }

    /**
     * @return Various types of cache configuration.
     */
    public static CacheConfiguration[] cacheConfigurations() {
        return F.asArray(
            cacheConfiguration(REPL_ATOMIC_CACHE, REPLICATED, ATOMIC, null),
            cacheConfiguration(REPL_TX_CACHE, REPLICATED, TRANSACTIONAL, null),
            cacheConfiguration(REPL_MVCC_CACHE, REPLICATED, TRANSACTIONAL_SNAPSHOT, "mvcc_repl_grp"),
            cacheConfiguration(PART_ATOMIC_CACHE, PARTITIONED, ATOMIC, "part_grp"),
            cacheConfiguration(PART_TX_CACHE, PARTITIONED, TRANSACTIONAL, "part_grp"),
            cacheConfiguration(PART_MVCC_CACHE, PARTITIONED, TRANSACTIONAL_SNAPSHOT, "mvcc_part_grp")
        );
    }

    /**
     * Asserts that all caches in read-only or in read/write mode on all nodes.
     *
     * @param readOnly If {@code true} then cache must be in read only mode, else in read/write mode.
     * @param cacheNames Checked cache names.
     */
    public static void assertCachesReadOnlyMode(boolean readOnly, Collection<String> cacheNames) {
        for (Ignite node : G.allGrids())
            assertCachesReadOnlyMode(node, readOnly, cacheNames);
    }

    /**
     * Asserts that all caches in read-only or in read/write mode. All cache operations will be performed from the
     * {@code node} node.
     *
     * @param node Node initiator cache operations.
     * @param readOnly If {@code true} then cache must be in read only mode, else in read/write mode.
     * @param cacheNames Checked cache names.
     */
    public static void assertCachesReadOnlyMode(Ignite node, boolean readOnly, Collection<String> cacheNames) {
        Random rnd = new Random();

        for (String cacheName : cacheNames) {
            IgniteCache<Integer, Integer> cache = node.cache(cacheName);

            for (int i = 0; i < 10; i++) {
                cache.get(rnd.nextInt(100)); // All gets must succeed.

                if (readOnly) {
                    // All puts must fail.
                    try {
                        cache.put(rnd.nextInt(100), rnd.nextInt());

                        fail("Put must fail for cache " + cacheName);
                    }
                    catch (Exception ignored) {
                        // No-op.
                    }

                    // All removes must fail.
                    try {
                        cache.remove(rnd.nextInt(100));

                        fail("Remove must fail for cache " + cacheName);
                    }
                    catch (Exception ignored) {
                        // No-op.
                    }
                }
                else {
                    int key = rnd.nextInt(100);

                    cache.put(key, rnd.nextInt()); // All puts must succeed.

                    cache.remove(key); // All removes must succeed.
                }
            }
        }
    }

    /**
     * @param readOnly If {@code true} then data streamer must fail, else succeed.
     * @param cacheNames Checked cache names.
     */
    public static void assertDataStreamerReadOnlyMode(boolean readOnly, Collection<String> cacheNames) {
        Random rnd = new Random();

        for (Ignite ignite : G.allGrids()) {
            for (String cacheName : cacheNames) {
                boolean failed = false;

                try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(cacheName)) {
                    for (int i = 0; i < 10; i++) {
                        ((DataStreamerImpl)streamer).maxRemapCount(5);

                        int key = rnd.nextInt(1000);

                        streamer.addData(key, rnd.nextInt());

                        streamer.removeData(key);
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

    /**
     * Checks that given {@code ex} exception has a cause of {@link IgniteClusterReadOnlyException}.
     *
     * @param ex Exception for the check.
     * @param name Name of object (optional).
     */
    public static void checkRootCause(Throwable ex, @Nullable String name) {
        if (!X.hasCause(ex, IgniteClusterReadOnlyException.class))
            throw new AssertionError("IgniteClusterReadOnlyException not found on " + name, ex);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param grpName Cache group name.
     */
    private static CacheConfiguration<Integer, Integer> cacheConfiguration(String name, CacheMode cacheMode,
        CacheAtomicityMode atomicityMode, String grpName) {
        return new CacheConfiguration<Integer, Integer>()
            .setName(name)
            .setCacheMode(cacheMode)
            .setAtomicityMode(atomicityMode)
            .setGroupName(grpName)
            .setBackups(1)
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)));
    }

    /** */
    private ClusterReadOnlyModeTestUtils() {
    }
}
