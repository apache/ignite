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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.function.Consumer;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CachePeekMode.ALL;

/**
 * Checks how operations under atomic cache works inside a transaction.
 */
public class AtomicOperationsInTxTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg  = new CacheConfiguration();

        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setName(DEFAULT_CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testPutInAllowedCache() {
        checkPut(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testPutInNotAllowedCache() {
        checkPut(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkOperation(boolean isAtomicCacheAllowedInTx,
        Consumer<IgniteCache<Integer, Integer>> op) {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        cache.clear();

        assertEquals(0, cache.size(ALL));

        IgniteException err = null;

        try (Transaction tx = grid(0).transactions().txStart()) {
            op.accept(cache);
        } catch (IgniteException e) {
            err = e;
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkPut(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.put(1, 1));
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testPutAsyncInAllowedCache() {
        checkPutAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testPutAsyncInNotAllowedCache() {
        checkPutAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkPutAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.putAsync(1, 1).get());
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testPutAllInAllowedCache() {
        checkPutAll(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testPutAllInNotAllowedCache() {
        checkPutAll(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkPutAll(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> {
            HashMap<Integer, Integer> map = new HashMap<>();

            map.put(1, 1);
            map.put(2, 1);

            cache.putAll(map);
        });
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testPutAllAsyncInAllowedCache() {
        checkPutAllAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testPutAllAsyncInNotAllowedCache() {
        checkPutAllAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkPutAllAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> {
            HashMap<Integer, Integer> map = new HashMap<>();

            map.put(1, 1);
            map.put(2, 1);

            cache.putAllAsync(map).get();
        });
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testPutIfAbsentInAllowedCache() {
        checkPutIfAbsentIn(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testPutIfAbsentInNotAllowedCache() {
        checkPutIfAbsentIn(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkPutIfAbsentIn(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.putIfAbsent(1, 1));
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testPutIfAbsentAsyncInAllowedCache() {
        checkPutIfAbsentAsyncIn(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testPutIfAbsentAsyncInNotAllowedCache() {
        checkPutIfAbsentAsyncIn(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkPutIfAbsentAsyncIn(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.putIfAbsentAsync(1, 1).get());
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testGetInAllowedCache() {
        checkGet(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testGetInNotAllowedCache() {
        checkGet(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkGet(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.get(1));
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testGetAllInAllowedCache() {
        checkGetAll(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testGetAllInNotAllowedCache() {
        checkGetAll(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkGetAll(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.getAll(set);
        });
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testGetAllAsyncInAllowedCache() {
        checkGetAllAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testGetAllAsyncInNotAllowedCache() {
        checkGetAllAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkGetAllAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> {
            HashSet<Integer> keys = new HashSet<>();

            keys.add(1);
            keys.add(2);

            cache.getAllAsync(keys).get();
        });
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testGetAndPutInAllowedCache() {
        checkGetAndPut(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testGetAndPutInNotAllowedCache() {
        checkGetAndPut(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkGetAndPut(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndPut(1, 2));
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testGetAndPutAsyncInAllowedCache() {
        checkGetAndPutAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testGetAndPutAsyncInNotAllowedCache() {
        checkGetAndPutAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkGetAndPutAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndPutAsync(1, 2).get());
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testGetAndPutIfAbsentInAllowedCache() {
        checkGetAndPutIfAbsent(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testGetAndPutIfAbsentInNotAllowedCache() {
        checkGetAndPutIfAbsent(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkGetAndPutIfAbsent(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndPutIfAbsent(1, 2));
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testGetAndPutIfAbsentAsyncInAllowedCache() {
        checkGetAndPutIfAbsentAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testGetAndPutIfAbsentAsyncInNotAllowedCache() {
        checkGetAndPutIfAbsentAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkGetAndPutIfAbsentAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndPutIfAbsentAsync(1, 2).get());
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testGetAndRemoveInAllowedCache() {
        checkGetAndRemove(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testGetAndRemoveInNotAllowedCache() {
        checkGetAndRemove(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkGetAndRemove(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndRemove(1));
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testGetAndRemoveAsyncInAllowedCache() {
        checkGetAndRemoveAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testGetAndRemoveAsyncInNotAllowedCache() {
        checkGetAndRemoveAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkGetAndRemoveAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndRemoveAsync(1));
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testGetAndReplaceInAllowedCache() {
        checkGetAndReplace(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testGetAndReplaceInNotAllowedCache() {
        checkGetAndReplace(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkGetAndReplace(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndReplace(1, 2));
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testGetAndReplaceAsyncInAllowedCache() {
        checkGetAndReplaceAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testGetAndReplaceAsyncInNotAllowedCache() {
        checkGetAndReplaceAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkGetAndReplaceAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndReplaceAsync(1, 2).get());
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testRemoveInAllowedCache() {
        checkRemove(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testRemoveInNotAllowedCache() {
        checkRemove(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkRemove(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.remove(1, 1));
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testRemoveAsyncInAllowedCache() {
        checkRemoveAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testRemoveAsyncInNotAllowedCache() {
        checkRemoveAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkRemoveAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.removeAsync(1, 1).get());
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testRemoveAllInAllowedCache() {
        checkRemoveAll(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testRemoveAllInNotAllowedCache() {
        checkRemoveAll(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkRemoveAll(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.removeAll(set);
        });
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testRemoveAllAsyncInAllowedCache() {
        checkRemoveAllAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testRemoveAllAsyncInNotAllowedCache() {
        checkRemoveAllAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkRemoveAllAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.removeAllAsync(set).get();
        });
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testContainsKeyInAllowedCache() {
        checkContainsKey(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testContainsKeyInNotAllowedcache() {
        checkContainsKey(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkContainsKey(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.containsKey(1));
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testContainsKeyAsyncInAllowedCache() {
        checkContainsKeyAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testContainsKeyAsyncInNotAllowedcache() {
        checkContainsKeyAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkContainsKeyAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.containsKeyAsync(1).get());
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testContainsKeysInAllowedCache() {
        checkContainsKeys(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testContainsKeysInNotAllowedcache() {
        checkContainsKeys(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkContainsKeys(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.containsKeys(set);
        });
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testContainsKeysAsyncInAllowedCache() {
        checkContainsKeysAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testContainsKeysAsyncInNotAllowedcache() {
        checkContainsKeysAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkContainsKeysAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.containsKeysAsync(set).get();
        });
    }

    /** */
    private class SetEntryProcessor implements EntryProcessor<Integer, Integer, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Integer> entry, Object... objects)
            throws EntryProcessorException {
            entry.setValue(entry.getKey());

            return null;
        }
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testInvokeInAllowedCache() {
        checkInvoke(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testInvokeInNotAllowedcache() {
        checkInvoke(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkInvoke(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.invoke(1, new SetEntryProcessor()));
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testInvokeAsyncInAllowedCache() {
        checkInvokeAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testInvokeAsyncInNotAllowedcache() {
        checkInvokeAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkInvokeAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.invokeAsync(1, new SetEntryProcessor()).get());
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testInvokeAllInAllowedCache() {
        checkInvokeAll(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testInvokeAllInNotAllowedcache() {
        checkInvokeAll(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkInvokeAll(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.invokeAll(set, new SetEntryProcessor());
        });
    }

    /**
     * Checks that atomic cache works inside a transaction.
     */
    public void testInvokeAllAsyncInAllowedCache() {
        checkInvokeAllAsync(true);
    }

    /**
     * Checks that operation throws exception inside a transaction.
     */
    public void testInvokeAllAsyncInNotAllowedcache() {
        checkInvokeAllAsync(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkInvokeAllAsync(boolean isAtomicCacheAllowedInTx) {
        checkOperation(isAtomicCacheAllowedInTx, cache -> {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.invokeAllAsync(set, new SetEntryProcessor()).get();
        });
    }
}
