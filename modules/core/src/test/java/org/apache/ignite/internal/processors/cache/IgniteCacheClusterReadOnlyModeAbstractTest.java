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
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Class with common logic for tests {@link IgniteCache} API when cluster in a {@link ClusterState#ACTIVE_READ_ONLY}
 * mode.
 */
public abstract class IgniteCacheClusterReadOnlyModeAbstractTest extends GridCommonAbstractTest {
    /** Key. */
    protected static final int KEY = 10;

    /** Value. */
    protected static final int VAL = 11;

    /** Key 2. */
    protected static final int KEY_2 = 20;

    /** Value 2. */
    protected static final int VAL_2 = 21;

    /** Key 3. */
    protected static final int KEY_3 = 30;

    /** Value 3. */
    protected static final int VAL_3 = 31;

    /** Unknown key. */
    protected static final int UNKNOWN_KEY = 3;

    /** Map with all pairs in caches. */
    protected static final Map<Integer, Integer> kvMap;

    /** */
    protected static final Predicate<CacheConfiguration> ATOMIC_CACHES_PRED = cfg -> cfg.getAtomicityMode() == ATOMIC;

    /** */
    protected static final Predicate<CacheConfiguration> TX_CACHES_PRED = cfg -> cfg.getAtomicityMode() == TRANSACTIONAL;

    /** */
    protected static final Predicate<CacheConfiguration> MVCC_CACHES_PRED = cfg -> cfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT;

    /** */
    protected static final Predicate<CacheConfiguration> NO_MVCC_CACHES_PRED = ATOMIC_CACHES_PRED.or(TX_CACHES_PRED);

    /** Started cache configurations. */
    protected static Collection<CacheConfiguration<?, ?>> cacheConfigurations;

    /** Started cache names. */
    protected static Collection<String> cacheNames;

    static {
        Map<Integer, Integer> map = new HashMap<>();

        map.put(KEY, VAL);
        map.put(KEY_2, VAL_2);
        map.put(KEY_3, VAL_3);

        kvMap = Collections.unmodifiableMap(map);
    }

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
            .setCacheConfiguration(cfgs);
    }

    /**
     * @return Cache configurations for node start.
     */
    protected CacheConfiguration<?, ?>[] cacheConfigurations() {
        CacheConfiguration<?, ?>[] cfgs = ClusterReadOnlyModeTestUtils.cacheConfigurations();

        for (CacheConfiguration cfg : cfgs)
            cfg.setReadFromBackup(true);

        return cfgs;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cacheNames = null;
        cacheConfigurations = null;

        startGrids(2);
        startClientGridsMultiThreaded(3, 2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        for (String cacheName : cacheNames) {
            grid(0).cache(cacheName).put(KEY, VAL);
            grid(0).cache(cacheName).put(KEY_2, VAL_2);
            grid(0).cache(cacheName).put(KEY_3, VAL_3);
        }

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cacheNames = null;
        cacheConfigurations = null;

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        commonChecks();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        commonChecks();

        super.afterTest();
    }

    /** */
    protected void performActionReadOnlyExceptionExpected(Consumer<IgniteCache<Integer, Integer>> clo) {
        performActionReadOnlyExceptionExpected(clo, null);
    }

    /** */
    protected void performActionReadOnlyExceptionExpected(
        Consumer<IgniteCache<Integer, Integer>> clo,
        @Nullable Predicate<CacheConfiguration> cfgFilter
    ) {
        performAction(
            cache -> {
                Throwable ex = assertThrows(log, () -> clo.accept(cache), Exception.class, null);

                ClusterReadOnlyModeTestUtils.checkRootCause(ex, cache.getName());
            },
            cfgFilter
        );
    }

    /** */
    protected void performAction(Consumer<IgniteCache<Integer, Integer>> clo) {
        performAction(clo, null);
    }

    /** */
    protected void performAction(BiConsumer<Ignite, IgniteCache<Integer, Integer>> clo) {
        performAction(clo, null);
    }

    /** */
    protected void performAction(
        Consumer<IgniteCache<Integer, Integer>> clo,
        @Nullable Predicate<CacheConfiguration> cfgFilter
    ) {
        performAction((node, cache) -> clo.accept(cache), cfgFilter);
    }

    /** */
    protected void performAction(
        BiConsumer<Ignite, IgniteCache<Integer, Integer>> clo,
        @Nullable Predicate<CacheConfiguration> cfgFilter
    ) {
        Collection<String> names = cfgFilter == null ?
            cacheNames :
            cacheConfigurations.stream()
                .filter(cfgFilter)
                .map(CacheConfiguration::getName)
                .collect(toList());

        for (Ignite node : G.allGrids()) {
            for (String name : names)
                clo.accept(node, node.cache(name));
        }
    }

    /** */
    private void commonChecks() {
        assertEquals(kvMap.toString(), 3, kvMap.size());

        for (Ignite node : G.allGrids()) {
            assertEquals(node.name(), ClusterState.ACTIVE_READ_ONLY, node.cluster().state());

            for (String cacheName : cacheNames) {
                IgniteCache<Integer, Integer> cache = node.cache(cacheName);

                assertEquals(node.name() + " " + cacheName, kvMap.size(), cache.size());

                for (Map.Entry<Integer, Integer> entry : kvMap.entrySet())
                    assertEquals(node.name() + " " + cacheName, entry.getValue(), cache.get(entry.getKey()));

                assertNull(node.name() + " " + cacheName, cache.get(UNKNOWN_KEY));
            }
        }
    }

    /** */
    protected static CacheConfiguration<?, ?>[] filterAndAddNearCacheConfig(CacheConfiguration<?, ?>[] cfgs) {
        return Stream.of(cfgs)
            // Near caches doesn't support in a MVCC mode.
            .filter(cfg -> cfg.getAtomicityMode() != CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            .map(cfg -> cfg.setNearConfiguration(new NearCacheConfiguration<>()))
            .toArray(CacheConfiguration[]::new);
    }
}
