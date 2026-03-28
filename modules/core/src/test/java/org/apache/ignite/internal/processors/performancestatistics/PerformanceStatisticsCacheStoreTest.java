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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_DELETE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_DELETE_ALL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_LOAD;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_LOAD_ALL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_LOAD_CACHE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_WRITE;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_WRITE_ALL;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

/** Tests performance statistics for cache store operations. */
@RunWith(Parameterized.class)
public class PerformanceStatisticsCacheStoreTest extends AbstractPerformanceStatisticsTest {
    /** Cache store operation types. */
    private static final Set<OperationType> CACHE_STORE_OPS = EnumSet.of(
        CACHE_LOAD,
        CACHE_LOAD_ALL,
        CACHE_LOAD_CACHE,
        CACHE_WRITE,
        CACHE_WRITE_ALL,
        CACHE_DELETE,
        CACHE_DELETE_ALL
    );

    /** Ignite. */
    private IgniteEx ignite;

    /** Test cache. */
    private IgniteCache<Integer, Integer> cache;

    /** Write-behind mode flag. */
    @Parameter
    public boolean writeBehind;

    /** */
    @Parameters(name = "writeBehind={0}")
    public static List<Boolean> parameters() {
        return List.of(false, true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(GridAbstractTest.<Integer, Integer>defaultCacheConfiguration()
            .setReadThrough(true)
            .setWriteThrough(true)
            .setWriteBehindEnabled(writeBehind)
            .setCacheStoreFactory(FactoryBuilder.factoryOf(TestCacheStore.class))
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ignite = startGrid(0);

        cache = ignite.cache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheStoreLoadOperation() throws Exception {
        checkCacheStoreOperations(Map.of(CACHE_LOAD, 1), () -> cache.get(0));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheStoreLoadAllOperation() throws Exception {
        checkCacheStoreOperations(Map.of(CACHE_LOAD_ALL, 1), () -> cache.getAll(Set.of(0, 1)));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheStoreLoadCacheOperation() throws Exception {
        checkCacheStoreOperations(Map.of(CACHE_LOAD_CACHE, 1), () -> cache.loadCache(null));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheStoreWriteDeleteOperation() throws Exception {
        checkCacheStoreOperations(Map.of(CACHE_WRITE, 1, CACHE_DELETE, 1), () -> {
            cache.put(0, 0);
            cache.remove(0);
        });
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheStoreWriteDeleteAllOperation() throws Exception {
        checkCacheStoreOperations(Map.of(CACHE_WRITE_ALL, 1, CACHE_DELETE_ALL, 1), () -> {
            Map<Integer, Integer> vals = Map.of(0, 0, 1, 1);

            cache.putAll(vals);
            cache.removeAll(vals.keySet());
        });
    }

    /** Checks expected store operations in performance statistics. */
    private void checkCacheStoreOperations(Map<OperationType, Integer> expOps, Runnable action) throws Exception {
        long startTime = U.currentTimeMillis();

        startCollectStatistics();

        action.run();

        Map<OperationType, Integer> actOps = new EnumMap<>(OperationType.class);

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long opStartTime, long duration) {
                if (cacheId != CU.cacheId(DEFAULT_CACHE_NAME) || !CACHE_STORE_OPS.contains(type))
                    return;

                actOps.merge(type, 1, Integer::sum);

                assertEquals(ignite.context().localNodeId(), nodeId);
                assertTrue(opStartTime >= startTime);
                assertTrue(duration >= 0);
            }
        });

        assertEquals("Unexpected operations for cache " + DEFAULT_CACHE_NAME, expOps, actOps);
    }

    /** Test cache store. */
    public static class TestCacheStore implements CacheStore<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo, @Nullable Object... args) throws CacheLoaderException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            return key;
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, Integer> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            Map<Integer, Integer> vals = new HashMap<>();

            for (Integer key : keys)
                vals.put(key, key);

            return vals;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Integer>> entries)
            throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            // No-op.
        }
    }
}
