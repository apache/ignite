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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;

/**
 * Checks that third party stores works fine if cluster in a {@link ClusterState#ACTIVE_READ_ONLY} state.
 */
public class IgniteCacheStoreClusterReadOnlyModeSelfTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Key. */
    private static final int KEY = 10;

    /** Value. */
    private static final int VAL = 11;

    /** */
    private static final Map<String, TestCacheStore> cacheStores = new ConcurrentHashMap<>();

    /** Started cache configurations. */
    private Collection<CacheConfiguration<?, ?>> cacheConfigurations;

    /** Started cache names. */
    private Collection<String> cacheNames;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<?, ?>[] cfgs = cacheConfigurations();

        cacheConfigurations = Collections.unmodifiableCollection(Arrays.asList(cfgs));

        cacheNames = Collections.unmodifiableSet(
            cacheConfigurations.stream()
                .map(CacheConfiguration::getName)
                .collect(toSet())
        );

        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setCacheConfiguration(cfgs);
    }

    /**
     * @return Cache configurations for node start.
     */
    protected CacheConfiguration<?, ?>[] cacheConfigurations() {
        return Stream.of(ClusterReadOnlyModeTestUtils.cacheConfigurations())
            // 3'rd party store doesn't support in a MVCC mode.
            .filter(cfg -> cfg.getAtomicityMode() != CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            .map(cfg -> cfg.setReadThrough(true))
            .map(cfg -> cfg.setWriteBehindEnabled(true))
            .map(cfg -> cfg.setWriteThrough(true))
            .map(cfg -> cfg.setCacheStoreFactory(new TestCacheStoreFactory(cfg.getName())))
            .toArray(CacheConfiguration[]::new);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cacheStores.clear();

        IgniteEx crd = startGrids(NODES_CNT);

        for (String name : cacheNames)
            crd.cache(name).put(KEY, VAL);

        crd.cluster().state(ACTIVE_READ_ONLY);

        for (Ignite node : G.allGrids())
            assertEquals(ACTIVE_READ_ONLY, node.cluster().state());

        assertEquals(NODES_CNT, crd.cluster().nodes().size());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cacheStores.clear();

        super.afterTest();
    }

    /** */
    @Test
    public void testLoadFromCacheStoreOnGetAllowed() {
        checkBefore();

        fillStores();

        for (String name : cacheNames) {
            for (Ignite node : G.allGrids()) {
                for (int i = 0; i < NODES_CNT; i++)
                    assertEquals(name, i, node.cache(name).get(i));
            }
        }

        checkAfter();
    }

    /** */
    @Test
    public void testLoadCacheAllowed() {
        checkBefore();

        fillStores();

        for (String name : cacheNames) {
            for (Ignite node : G.allGrids()) {
                for (int i = 0; i < NODES_CNT; i++) {
                    int j = i;

                    node.cache(name).loadCache((k, v) -> ((int)k) % NODES_CNT == j);
                }
            }
        }

        checkAfter();
    }

    /** */
    @Test
    public void testLoadCacheAsyncAllowed() {
        checkBefore();

        fillStores();

        for (String name : cacheNames) {
            for (Ignite node : G.allGrids()) {
                for (int i = 0; i < NODES_CNT; i++) {
                    int j = i;

                    node.cache(name).loadCacheAsync((k, v) -> ((int)k) % NODES_CNT == j).get();
                }
            }
        }

        checkAfter();
    }

    /** */
    @Test
    public void testLocalLoadCacheAllowed() {
        checkBefore();

        fillStores();

        for (String name : cacheNames) {
            for (Ignite node : G.allGrids()) {
                for (int i = 0; i < NODES_CNT; i++)
                    node.cache(name).localLoadCache(null);
            }
        }

        checkAfter();
    }

    /** */
    @Test
    public void testLocalLoadCacheAsyncAllowed() {
        checkBefore();

        fillStores();

        for (String name : cacheNames) {
            for (Ignite node : G.allGrids()) {
                for (int i = 0; i < NODES_CNT; i++)
                    node.cache(name).localLoadCacheAsync(null).get();
            }
        }

        checkAfter();
    }

    /** */
    private void checkAfter() {
        for (String name : cacheNames) {
            for (Ignite node : G.allGrids()) {
                IgniteCache<Object, Object> cache = node.cache(name);

                cache.loadCache(null);

                assertEquals(name, VAL, cache.get(KEY));

                for (int i = 0; i < NODES_CNT; i++)
                    assertEquals(name + " " + node.name(), i, cache.get(i));

                assertEquals(name, NODES_CNT + 1, cache.size());
            }
        }
    }

    /** */
    private void checkBefore() {
        for (String name : cacheNames) {
            for (Ignite node : G.allGrids()) {
                assertEquals(name, VAL, node.cache(name).get(KEY));
                assertEquals(name, 1, node.cache(name).size());

                for (int i = 0; i < NODES_CNT; i++)
                    assertNull(name, node.cache(name).get(i));
            }
        }
    }

    /** */
    private void fillStores() {
        for (TestCacheStore store : cacheStores.values()) {
            for (int i = 0; i < NODES_CNT; i++)
                store.map.put(i, i);
        }
    }

    /**
     *
     */
    private static class TestCacheStoreFactory implements Factory<CacheStore<Integer, Integer>> {
        /** Cache name. */
        private final String name;

        /** */
        private TestCacheStoreFactory(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public CacheStore<Integer, Integer> create() {
            return cacheStores.computeIfAbsent(name, k -> new TestCacheStore());
        }
    }

    /**
     *
     */
    private static class TestCacheStore extends CacheStoreAdapter<Integer, Integer> {
        /** Map storage. */
        private final Map<Integer, Integer> map = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(
            Cache.Entry<? extends Integer, ? extends Integer> entry
        ) throws CacheWriterException {
            map.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            map.remove(key);
        }
    }
}
