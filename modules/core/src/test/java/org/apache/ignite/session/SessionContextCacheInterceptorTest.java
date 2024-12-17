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

package org.apache.ignite.session;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.SessionContextProviderResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/** */
@RunWith(Parameterized.class)
public class SessionContextCacheInterceptorTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS = 10;

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode mode;

    /** */
    @Parameterized.Parameter(1)
    public CacheWriteSynchronizationMode syncMode;

    /** */
    @Parameterized.Parameter(2)
    public boolean clnNode;

    /** */
    @Parameterized.Parameter(3)
    public boolean dynamicCache;

    /** */
    @Parameterized.Parameter(4)
    public int backups;

    /** */
    private Ignite ign;

    /** */
    private IgniteCache<Integer, String> cache;

    /** */
    @Parameterized.Parameters(name = "mode={0}, syncMode={1}, clnNode={2}, dynamicCache={3}, backups={4}")
    public static Collection<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode m: CacheAtomicityMode.values()) {
            for (CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
                for (boolean cln : new boolean[] {true, false}) {
                    for (boolean dynCache : new boolean[] {true, false}) {
                        if (m == CacheAtomicityMode.TRANSACTIONAL) {
                            for (int backupCnt = 0; backupCnt <= 2; backupCnt++)
                                params.add(new Object[] {m, syncMode, cln, dynCache, backupCnt});
                        }
                        else
                            params.add(new Object[] {m, syncMode, cln, dynCache, 1});
                    }
                }
            }
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        Collection<Object[]> params = params();

        List<CacheConfiguration<?, ?>> ccfgs = new ArrayList<>();

        for (Object[] param : params) {
            boolean cln = (boolean)param[2];
            boolean dyn = (boolean)param[3];

            if (!dyn && !cln) {
                ccfgs.add(
                    cacheConfig((CacheAtomicityMode)param[0], (CacheWriteSynchronizationMode)param[1], (int)param[4], false, false)
                );
            }
        }

        cfg.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration<?, ?>[0]));

        return cfg;
    }

    /** */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);

        startClientGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ign = grid(0);

        if (clnNode)
            ign = grid(3);

        cache = ign.getOrCreateCache(cacheConfig(mode, syncMode, backups, dynamicCache, clnNode));

        cache.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testGetOperations() {
        // CacheInterceptor#onGet doesn't work for client nodes.
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-23810", clnNode);

        for (int i = 0; i < KEYS; i++)
            cache.put(i, String.valueOf(i));

        IgniteCache<Integer, String> cacheApp = ign
            .withApplicationAttributes(F.asMap("onGet", "sessionOnGet"))
            .cache(cacheName());

        for (int i = 0; i < KEYS; i++) {
            final int j = i;

            assertEquals("sessionOnGet" + j, () -> cacheApp.get(j));
            assertEquals("sessionOnGet" + j, () -> cacheApp.getEntry(j).getValue());

            assertEquals("sessionOnGet" + j, () -> cacheApp.getAsync(j).get(getTestTimeout()));
            assertEquals("sessionOnGet" + j, () -> cacheApp.getEntryAsync(j).get(getTestTimeout()).getValue());

            assertEquals(String.valueOf(j), () -> cache.get(j));
        }

        Set<Integer> keys = IntStream.range(0, KEYS).boxed().collect(Collectors.toSet());

        Map<Integer, String> getAll = cacheApp.getAll(keys);
        Map<Integer, String> getAllAsync = cacheApp.getAllAsync(keys).get(getTestTimeout());

        for (int i = 0; i < KEYS; i++) {
            final int j = i;

            assertEquals("sessionOnGet" + j, () -> getAll.get(j));
            assertEquals("sessionOnGet" + j, () -> getAllAsync.get(j));
        }

        Collection<CacheEntry<Integer, String>> entries = cacheApp.getEntries(keys);
        Collection<CacheEntry<Integer, String>> entriesAsync = cacheApp.getEntriesAsync(keys).get(getTestTimeout());

        for (CacheEntry<Integer, String> entry: entries)
            assertTrue(entry.getValue().contains("sessionOnGet"));

        for (CacheEntry<Integer, String> entry: entriesAsync)
            assertTrue(entry.getValue().contains("sessionOnGet"));
    }

    /** */
    @Test
    public void testGetAndPut() {
        IgniteCache<Integer, String> cacheApp = ign
            .withApplicationAttributes(F.asMap("onBeforePut", "sessionOnPut"))
            .cache(cacheName());

        for (int i = 0; i < KEYS; i++) {
            cacheApp.getAndPut(i, String.valueOf(i));
            cacheApp.getAndPutIfAbsent(KEYS + i, String.valueOf(i));

            cacheApp.getAndPutAsync(2 * KEYS + i, String.valueOf(i)).get(getTestTimeout());
            cacheApp.getAndPutIfAbsentAsync(3 * KEYS + i, String.valueOf(i)).get(getTestTimeout());
        }

        for (int i = 0; i < 4 * KEYS; i++) {
            final int j = i;

            assertEquals("sessionOnPut" + j, () -> cacheApp.get(j));
        }
    }

    /** */
    @Test
    public void testGetAndReplace() {
        for (int i = 0; i < 2 * KEYS; i++)
            cache.put(i, String.valueOf(i));

        IgniteCache<Integer, String> cacheApp = ign
            .withApplicationAttributes(F.asMap("onBeforePut", "sessionOnPut"))
            .cache(cacheName());

        for (int i = 0; i < KEYS; i++) {
            final int j = i;

            // Wait while FULL_ASYNC put finishes.
            assertEquals(String.valueOf(j), () -> cache.get(j));
            assertEquals(String.valueOf(KEYS + j), () -> cache.get(KEYS + j));

            String ret = cacheApp.getAndReplace(i, "0" + i);
            String retAsync = cacheApp.getAndReplaceAsync(KEYS + i, "1" + i).get(getTestTimeout());

            if (syncMode != CacheWriteSynchronizationMode.FULL_ASYNC) {
                assertEquals(String.valueOf(i), ret);
                assertEquals(String.valueOf(KEYS + i), retAsync);
            }

            assertEquals("sessionOnPut" + j, () -> cacheApp.get(j));
            assertEquals("sessionOnPut" + (KEYS + j), () -> cacheApp.get(KEYS + j));
        }
    }

    /** */
    @Test
    public void testPutOperations() {
        IgniteCache<Integer, String> cacheApp = ign
            .withApplicationAttributes(F.asMap("onBeforePut", "sessionOnPut"))
            .cache(cacheName());

        for (int i = 0; i < KEYS; i++) {
            cacheApp.put(i, String.valueOf(i));
            cacheApp.putAsync(KEYS + i, String.valueOf(i)).get(getTestTimeout());

            cacheApp.putIfAbsent(KEYS * 2 + i, String.valueOf(i));
            cacheApp.putIfAbsentAsync(KEYS * 3 + i, String.valueOf(i));
        }

        Map<Integer, String> all = IntStream.range(KEYS * 4, KEYS * 5)
            .boxed()
            .collect(Collectors.toMap(k -> k, String::valueOf));

        Map<Integer, String> allAsync = IntStream.range(KEYS * 5, KEYS * 6)
            .boxed()
            .collect(Collectors.toMap(k -> k, String::valueOf));

        cacheApp.putAll(all);
        cacheApp.putAllAsync(allAsync).get(getTestTimeout());

        for (int i = 0; i < KEYS * 6; i++) {
            final int j = i;

            assertEquals("sessionOnPut" + j, () -> cache.get(j));
        }
    }

    /** */
    @Test
    public void testOnBeforeRemove() {
        for (int i = 0; i < KEYS; i++)
            cache.put(i, String.valueOf(i));

        IgniteCache<Integer, String> cacheApp = ign
            .withApplicationAttributes(F.asMap("onBeforeRemove", "sessionOnRemove"))
            .cache(cacheName());

        for (int i = 0; i < KEYS; i++) {
            final int j = i;

            // Wait while FULL_ASYNC put finishes.
            assertEquals(String.valueOf(j), () -> cache.get(j));

            boolean removed = cacheApp.remove(i);

            // Transaction actually return other value. Because the interception performs at commit time.
            // FULL_ASYNC mode doesn't care about return value.
            if (mode != CacheAtomicityMode.TRANSACTIONAL && syncMode != CacheWriteSynchronizationMode.FULL_ASYNC)
                assertFalse(removed);

            cacheApp.remove(i);
            assertEquals(String.valueOf(j), () -> cache.get(j));
        }

        Set<Integer> keys = IntStream.range(0, KEYS).boxed().collect(Collectors.toSet());
        cacheApp.removeAll(keys);
        cacheApp.removeAllAsync(keys).get(getTestTimeout());

        for (int i = 0; i < KEYS; i++) {
            final int j = i;

            assertEquals(String.valueOf(j), () -> cache.get(j));
        }
    }

    /** */
    @Test
    public void testMultiCacheTransaction() throws Exception {
        assumeTrue(mode == CacheAtomicityMode.TRANSACTIONAL);

        ign.createCache(new CacheConfiguration<Integer, String>()
            .setName(cacheName("new-cache"))
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setInterceptor(new SessionContextCacheInterceptor()));

        awaitPartitionMapExchange();

        Ignite ignApp = ign
            .withApplicationAttributes(F.asMap("onBeforePut", "sessionOnPut"));

        int mult = 0;

        for (TransactionConcurrency txConc: TransactionConcurrency.values()) {
            for (TransactionIsolation txIsol: TransactionIsolation.values()) {
                for (boolean async: new boolean[] {false, true}) {
                    try (Transaction tx = ignApp.transactions().txStart(txConc, txIsol)) {
                        for (int i = mult * KEYS; i < (mult + 1) * KEYS; i++) {
                            ignApp.cache(cacheName()).put(i, String.valueOf(i));
                            ignApp.cache(cacheName("new-cache")).put(i, String.valueOf(i));
                        }

                        if (async)
                            tx.commitAsync().get(getTestTimeout());
                        else
                            tx.commit();

                        mult++;
                    }
                }
            }
        }

        for (int i = 0; i < mult * KEYS; i++) {
            final int j = i;

            assertEquals("sessionOnPut" + j, () -> cache.get(j));
            assertEquals("sessionOnPut" + j, () -> ign.cache(cacheName("new-cache")).get(j));
        }
    }

    /** */
    @Test
    public void testExplicitSingleKeyTransaction() {
        assumeTrue(mode == CacheAtomicityMode.TRANSACTIONAL);

        Ignite ignApp = ign
            .withApplicationAttributes(F.asMap("onBeforePut", "sessionOnPut"));

        int key = 0;

        for (TransactionConcurrency txConc: TransactionConcurrency.values()) {
            for (TransactionIsolation txIsol: TransactionIsolation.values()) {
                for (boolean async: new boolean[] {false, true}) {
                    try (Transaction tx = ignApp.transactions().txStart(txConc, txIsol)) {
                        ignApp.cache(cacheName()).put(key, String.valueOf(key));

                        if (async)
                            tx.commitAsync().get(getTestTimeout());
                        else
                            tx.commit();

                        key++;
                    }
                }
            }
        }

        for (int i = 0; i < key; i++) {
            final int j = i;

            assertEquals("sessionOnPut" + j, () -> cache.get(j));
        }
    }

    /** */
    @Test
    public void testMultipleApplicationAttributes() {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-23810", clnNode);

        IgniteCache<Integer, String> cacheApp = ign
            .withApplicationAttributes(F.asMap(
                "onGet", "sessionOnGet",
                "onBeforePut", "sessionOnPut"))
            .cache(cacheName());

        cache.put(0, "0");

        assertEquals("sessionOnGet" + 0, () -> cacheApp.get(0));

        cacheApp.put(1, "1");

        assertEquals("sessionOnPut" + 1, () -> cache.get(1));
    }

    /** */
    @Test
    public void testWithNoRetries() {
        IgniteCache<Object, Object> cacheApp = ign
            .withApplicationAttributes(F.asMap("onBeforePut", "sessionOnPut"))
            .cache(cacheName())
            .withNoRetries();

        for (int i = 0; i < KEYS; i++)
            cacheApp.putAsync((i), String.valueOf(i)).get(getTestTimeout());

        for (int i = 0; i < KEYS; i++) {
            final int j = i;

            assertEquals("sessionOnPut" + j, () -> cache.get(j));
        }
    }

    /** */
    private void assertEquals(Object exp, Supplier<Object> act) {
        if (syncMode == CacheWriteSynchronizationMode.FULL_SYNC)
            assertEquals(exp, act.get());
        else {
            try {
                assertTrue(GridTestUtils.waitForCondition(() -> exp.equals(act.get()), getTestTimeout(), 10L));
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** */
    private static final class SessionContextCacheInterceptor implements CacheInterceptor<Integer, String> {
        /** */
        @SessionContextProviderResource
        private SessionContextProvider sesCtxPrv;

        /** */
        @Override public @Nullable String onGet(Integer key, @Nullable String val) {
            String ret = sesCtxPrv.getSessionContext().getAttribute("onGet");

            return ret == null ? val : ret + key;
        }

        /** */
        @Override public @Nullable String onBeforePut(Cache.Entry<Integer, String> entry, String newVal) {
            String ret = sesCtxPrv.getSessionContext().getAttribute("onBeforePut");

            return ret == null ? newVal : ret + entry.getKey();
        }

        /** */
        @Override public void onAfterPut(Cache.Entry<Integer, String> entry) {

        }

        /** */
        @Override public @Nullable IgniteBiTuple<Boolean, String> onBeforeRemove(Cache.Entry<Integer, String> entry) {
            String ret = sesCtxPrv.getSessionContext().getAttribute("onBeforeRemove");;

            return new IgniteBiTuple<>(ret != null, entry.getValue());
        }

        /** */
        @Override public void onAfterRemove(Cache.Entry<Integer, String> entry) {

        }
    }

    /** */
    private String cacheName() {
        return cacheName(DEFAULT_CACHE_NAME);
    }

    /** */
    private String cacheName(String baseName) {
        return cacheName(baseName, mode, syncMode, backups, dynamicCache, clnNode);
    }

    /** */
    private String cacheName(
        String baseName,
        CacheAtomicityMode mode,
        CacheWriteSynchronizationMode syncMode,
        int backups,
        boolean dynamicCache,
        boolean clnNode
    ) {
        return baseName + "_" + mode + "_" + syncMode + "_" + backups + "_" + dynamicCache + "_" + clnNode;
    }

    /** */
    private CacheConfiguration<Integer, String> cacheConfig(
        CacheAtomicityMode mode,
        CacheWriteSynchronizationMode syncMode,
        int backups,
        boolean dynamicCache,
        boolean clnNode
    ) {
        return new CacheConfiguration<Integer, String>()
            .setName(cacheName(DEFAULT_CACHE_NAME, mode, syncMode, backups, dynamicCache, clnNode))
            .setAtomicityMode(mode)
            .setWriteSynchronizationMode(syncMode)
            .setBackups(backups)
            .setInterceptor(new SessionContextCacheInterceptor());
    }
}
