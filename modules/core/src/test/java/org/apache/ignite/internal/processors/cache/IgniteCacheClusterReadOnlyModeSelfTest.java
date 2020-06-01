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
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheConfigurations;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheNames;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/**
 *
 */
public class IgniteCacheClusterReadOnlyModeSelfTest extends GridCommonAbstractTest {
    /** Key. */
    private static final int KEY = 10;

    /** Value. */
    private static final int VAL = 11;

    /** Key 2. */
    private static final int KEY_2 = 20;

    /** Value 2. */
    private static final int VAL_2 = 21;

    /** Key 3. */
    private static final int KEY_3 = 30;

    /** Value 3. */
    private static final int VAL_3 = 31;

    /** Map with all pairs in caches. */
    private static final Map<Integer, Integer> kvMap = new HashMap<>();

    /** Unknown key. */
    private static final int UNKNOWN_KEY = 3;

    static {
        kvMap.put(KEY, VAL);
        kvMap.put(KEY_2, VAL_2);
        kvMap.put(KEY_3, VAL_3);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(ClusterReadOnlyModeTestUtils.cacheConfigurations());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
        startClientGridsMultiThreaded(3, 2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        for (String cacheName : cacheNames()) {
            grid(0).cache(cacheName).put(KEY, VAL);
            grid(0).cache(cacheName).put(KEY_2, VAL_2);
            grid(0).cache(cacheName).put(KEY_3, VAL_3);
        }

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

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
    @Test
    public void testGetAndPutIfAbsentDeniedIfKeyIsPresented() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutIfAbsent(KEY, VAL));
    }

    /** */
    @Test
    public void testGetAndPutIfAbsentDeniedIfKeyIsAbsent() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutIfAbsent(UNKNOWN_KEY, VAL));
    }

    /** */
    @Test
    public void testGetAndPutIfAbsentAsyncDeniedIfKeyIsPresented() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutIfAbsentAsync(KEY, VAL).get());
    }

    /** */
    @Test
    public void testGetAndPutIfAbsentAsyncDeniedIfKeyIsAbsent() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutIfAbsentAsync(UNKNOWN_KEY, VAL).get());
    }

    /** */
    @Test
    public void testLockDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.lock(KEY).lock(), cfg -> cfg.getAtomicityMode() == TRANSACTIONAL);
    }

    /** */
    @Test
    public void testLockAllDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.lockAll(asList(KEY, KEY_2)).lock(), cfg -> cfg.getAtomicityMode() == TRANSACTIONAL);
    }

    /** */
    @Test
    public void testScanQueryAllowed() {
        performAction(cache -> {
            try (QueryCursor qry = cache.query(new ScanQuery<>())) {
                for (Object o : qry.getAll()) {
                    IgniteBiTuple<Integer, Integer> tuple = (IgniteBiTuple<Integer, Integer>)o;

                    assertEquals(o.toString(), kvMap.get(tuple.getKey()), tuple.getValue());
                }
            }
        });
    }

    /** */
    @Test
    public void testSizeAllowed() {
        performAction(cache -> assertEquals(kvMap.size(), cache.size()));
    }

    /** */
    @Test
    public void testSizeAsyncAllowed() {
        performAction(cache -> assertEquals(kvMap.size(), (long)cache.sizeAsync().get()));
    }

    /** */
    @Test
    public void testSizeLongAllowed() {
        performAction(cache -> assertEquals(kvMap.size(), cache.sizeLong()));
    }

    /** */
    @Test
    public void testSizeLongAsyncAllowed() {
        performAction(cache -> assertEquals(kvMap.size(), (long)cache.sizeLongAsync().get()));
    }

    /** */
    @Test
    public void testGetAllowed() {
        performAction(cache -> assertEquals(VAL, (int)cache.get(KEY)));
        performAction(cache -> assertNull(cache.get(UNKNOWN_KEY)));
    }

    /** */
    @Test
    public void testGetAsyncAllowed() {
        performAction(cache -> assertEquals(VAL, (int)cache.getAsync(KEY).get()));
        performAction(cache -> assertNull(cache.getAsync(UNKNOWN_KEY).get()));
    }

    /** */
    @Test
    public void testGetEntryAllowed() {
        performAction(cache -> {
            CacheEntry entry = cache.getEntry(KEY);

            assertEquals(KEY, entry.getKey());
            assertEquals(VAL, entry.getValue());
        });

        performAction(cache -> assertNull(cache.getEntry(UNKNOWN_KEY)));
    }

    /** */
    @Test
    public void testGetEntryAsyncAllowed() {
        performAction(cache -> {
            CacheEntry entry = cache.getEntryAsync(KEY).get();

            assertEquals(KEY, entry.getKey());
            assertEquals(VAL, entry.getValue());
        });

        performAction(cache -> assertNull(cache.getEntryAsync(UNKNOWN_KEY).get()));
    }

    /** */
    @Test
    public void testGetAllAllowed() {
        performAction(cache -> assertEquals(kvMap, cache.getAll(kvMap.keySet())));
    }

    /** */
    @Test
    public void testGetAllAsyncAllowed() {
        performAction(cache -> assertEquals(kvMap, (cache.getAllAsync(kvMap.keySet()).get())));
    }

    /** */
    @Test
    public void testGetEntriesAllowed() {
        performAction(cache -> {
            for (Object o : cache.getEntries(kvMap.keySet())) {
                CacheEntry<Integer, Integer> entry = (CacheEntry<Integer, Integer>)o;

                assertEquals(kvMap.get(entry.getKey()), entry.getValue());
            }
        });
    }

    /** */
    @Test
    public void testGetEntriesAsyncAllowed() {
        performAction(cache -> {
            for (Object o : cache.getEntriesAsync(kvMap.keySet()).get()) {
                CacheEntry<Integer, Integer> entry = (CacheEntry<Integer, Integer>)o;

                assertEquals(kvMap.get(entry.getKey()), entry.getValue());
            }
        });
    }

    /** */
    @Test
    public void testGetAllOutTxAllowed() {
        performAction(
            (node, cache) -> {
                for (TransactionConcurrency level : TransactionConcurrency.values()) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        CacheConfiguration cfg = cache.getConfiguration(CacheConfiguration.class);

                        if (level == OPTIMISTIC && cfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT) {
                            // Only pessimistic transactions are supported when MVCC is enabled.
                            continue;
                        }

                        Transaction tx = node.transactions().txStart(level, isolation);

                        try {
                            cache.get(UNKNOWN_KEY);

                            assertEquals(kvMap, cache.getAllOutTx(kvMap.keySet()));

                            tx.commit();
                        }
                        catch (Exception e) {
                            RuntimeException ex = new RuntimeException(new AssertionError(
                                "Got exception on node: " + node.name() + " cache: " + cache.getName() + " isolation: " + isolation + " txLevel: " + level,
                                e
                            ));

                            log.error("", ex);

                            tx.rollback();

                            throw ex;
                        }
                    }
                }
            },
            cfg -> cfg.getAtomicityMode() != ATOMIC
        );
    }

    /** */
    @Test
    public void testGetAllOutTxAsyncAllowed() {
        performAction(
            (node, cache) -> {
                for (TransactionConcurrency level : TransactionConcurrency.values()) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        CacheConfiguration cfg = cache.getConfiguration(CacheConfiguration.class);

                        if (level == OPTIMISTIC && cfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT) {
                            // Only pessimistic transactions are supported when MVCC is enabled.
                            continue;
                        }

                        Transaction tx = node.transactions().txStart(level, isolation);

                        try {
                            cache.get(UNKNOWN_KEY);

                            assertEquals(kvMap, cache.getAllOutTxAsync(kvMap.keySet()).get());

                            tx.commit();
                        }
                        catch (Exception e) {
                            RuntimeException ex = new RuntimeException(new AssertionError(
                                "Got exception on node: " + node.name() + " cache: " + cache.getName() + " isolation: " + isolation + " txLevel: " + level,
                                e
                            ));

                            log.error("", ex);

                            tx.rollback();

                            throw ex;
                        }
                    }
                }
            },
            cfg -> cfg.getAtomicityMode() != ATOMIC
        );
    }

    /** */
    @Test
    public void testContainsKeyAllowed() {
        performAction(cache -> assertTrue(KEY + "", cache.containsKey(KEY)));
        performAction(cache -> assertFalse(UNKNOWN_KEY + "", cache.containsKey(UNKNOWN_KEY)));
    }

    /** */
    @Test
    public void testContainsKeyAsyncAllowed() {
        performAction(cache -> assertTrue(KEY + "", cache.containsKeyAsync(KEY).get()));
        performAction(cache -> assertFalse(UNKNOWN_KEY + "", cache.containsKeyAsync(UNKNOWN_KEY).get()));
    }

    /** */
    @Test
    public void testContainsKeysAllowed() {
        performAction(cache -> assertTrue(valueOf(kvMap.keySet()), cache.containsKeys(kvMap.keySet())));
        performAction(cache -> assertFalse(UNKNOWN_KEY + "", cache.containsKeys(singleton(UNKNOWN_KEY))));
    }

    /** */
    @Test
    public void testContainsKeysAsyncAllowed() {
        performAction(cache -> assertTrue(valueOf(kvMap.keySet()), cache.containsKeysAsync(kvMap.keySet()).get()));
        performAction(cache -> assertFalse(UNKNOWN_KEY + "", cache.containsKeysAsync(singleton(UNKNOWN_KEY)).get()));
    }

    /** */
    @Test
    public void testPutDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.put(KEY, VAL + 1));
        performActionReadOnlyExceptionExpected(cache -> cache.put(UNKNOWN_KEY, 777));
    }

    /** */
    @Test
    public void testPutAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.putAsync(KEY, VAL + 1).get());
        performActionReadOnlyExceptionExpected(cache -> cache.putAsync(UNKNOWN_KEY, 777).get());
    }

    /** */
    @Test
    public void testGetAndPutDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPut(KEY, VAL + 1));
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPut(UNKNOWN_KEY, 777));
    }

    /** */
    @Test
    public void testGetAndPutAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutAsync(KEY, VAL + 1).get());
        performActionReadOnlyExceptionExpected(cache -> cache.getAndPutAsync(UNKNOWN_KEY, 777).get());
    }

    /** */
    @Test
    public void testPutAllDenied() {
        Map<Integer, Integer> newMap = kvMap.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> e.getValue() + 1));

        performActionReadOnlyExceptionExpected(cache -> cache.putAll(newMap));
    }

    /** */
    @Test
    public void testPutAllAsyncDenied() {
        Map<Integer, Integer> newMap = kvMap.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> e.getValue() + 1));

        performActionReadOnlyExceptionExpected(cache -> cache.putAllAsync(newMap).get());
    }

    /** */
    @Test
    public void testPutIfAbsentDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.putIfAbsent(KEY, VAL + 1));
        performActionReadOnlyExceptionExpected(cache -> cache.putIfAbsent(UNKNOWN_KEY, 777));
    }

    /** */
    @Test
    public void testPutIfAbsentAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.putIfAbsentAsync(KEY, VAL + 1).get());
        performActionReadOnlyExceptionExpected(cache -> cache.putIfAbsentAsync(UNKNOWN_KEY, 777).get());
    }

    /** */
    @Test
    public void testRemoveDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.remove(KEY));
        performActionReadOnlyExceptionExpected(cache -> cache.remove(UNKNOWN_KEY));
    }

    /** */
    private void performActionReadOnlyExceptionExpected(Consumer<IgniteCache<Integer, Integer>> clo) {
        performActionReadOnlyExceptionExpected(clo, null);
    }

    /** */
    private void performActionReadOnlyExceptionExpected(
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
    private void performAction(Consumer<IgniteCache<Integer, Integer>> clo) {
        performAction(clo, null);
    }

    /** */
    private void performAction(
        Consumer<IgniteCache<Integer, Integer>> clo,
        @Nullable Predicate<CacheConfiguration> cfgFilter
    ) {
        performAction((node, cache) -> clo.accept(cache), cfgFilter);
    }

    /** */
    private void performAction(
        BiConsumer<Ignite, IgniteCache<Integer, Integer>> clo,
        @Nullable Predicate<CacheConfiguration> cfgFilter
    ) {
        Collection<String> cacheNames = cfgFilter == null ?
            cacheNames() :
            Stream.of(cacheConfigurations()).filter(cfgFilter).map(CacheConfiguration::getName).collect(toList());

        for (Ignite node : G.allGrids()) {
            for (String cacheName : cacheNames)
                clo.accept(node, node.cache(cacheName));
        }
    }

    /** */
    private void commonChecks() {
        assertEquals(kvMap.toString(), 3, kvMap.size());

        for (Ignite node : G.allGrids()) {
            assertEquals(node.name(), ClusterState.ACTIVE_READ_ONLY, node.cluster().state());

            for (String cacheName : cacheNames()) {
                IgniteCache<Integer, Integer> cache = node.cache(cacheName);

                assertEquals(node.name() + " " + cacheName, kvMap.size(), cache.size());

                for (Map.Entry<Integer, Integer> entry : kvMap.entrySet())
                    assertEquals(node.name() + " " + cacheName, entry.getValue(), cache.get(entry.getKey()));

                assertNull(node.name() + " " + cacheName, cache.get(UNKNOWN_KEY));
            }
        }
    }
}
