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
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

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
    private void checkPut(boolean isAtomicCacheAllowedInTx) {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.remove(1);

        assertNull(cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.put(1, 1);
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.remove(1);

        assertNull(cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.putAsync(1, 1).get();
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.remove(1);
        cache.remove(2);

        assertNull(cache.get(1));
        assertNull(cache.get(2));

        try (Transaction tx = grid(0).transactions().txStart()) {
            HashMap<Integer, Integer> map = new HashMap<>();

            map.put(1, 1);
            map.put(2, 1);

            cache.putAll(map);
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
            cache.remove(2);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.remove(1);
        cache.remove(2);

        assertNull(cache.get(1));
        assertNull(cache.get(2));

        try (Transaction tx = grid(0).transactions().txStart()) {
            HashMap<Integer, Integer> map = new HashMap<>();

            map.put(1, 1);
            map.put(2, 1);

            cache.putAllAsync(map).get();
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
            cache.remove(2);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.remove(1);

        assertNull(cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.putIfAbsent(1, 1);
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.remove(1);

        assertNull(cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.putIfAbsentAsync(1, 1).get();
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.get(1);
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);
        cache.put(2, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.getAll(set);
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
            cache.remove(2);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);
        cache.put(2, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            HashSet<Integer> keys = new HashSet<>();

            keys.add(1);
            keys.add(2);

            cache.getAllAsync(keys).get();
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
            cache.remove(2);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.getAndPut(1, 2);
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.getAndPutAsync(1, 2).get();
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.remove(1);

        assertNull(cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.getAndPutIfAbsent(1, 2);
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.remove(1);

        assertNull(cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.getAndPutIfAbsentAsync(1, 2).get();
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.getAndRemove(1);
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.getAndRemoveAsync(1);
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.getAndReplace(1, 2);
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.getAndReplaceAsync(1, 2).get();
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.remove(1, 1);
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.removeAsync(1, 1).get();
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);
        cache.put(2, 1);

        assertEquals((Integer) 1, cache.get(1));
        assertEquals((Integer) 1, cache.get(2));

        try (Transaction tx = grid(0).transactions().txStart()) {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.removeAll(set);
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
            cache.remove(2);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);
        cache.put(2, 1);

        assertEquals((Integer) 1, cache.get(1));
        assertEquals((Integer) 1, cache.get(2));

        try (Transaction tx = grid(0).transactions().txStart()) {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.removeAllAsync(set).get();
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
            cache.remove(2);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.containsKey(1);
        } catch (IgniteException e) {
            err = e;
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage().
                contains("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.containsKeyAsync(1).get();
        } catch (IgniteException e) {
            err = e;
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage().
                contains("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        try (Transaction tx = grid(0).transactions().txStart()) {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.containsKeys(set);
        } catch (IgniteException e) {
            err = e;
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage().
                contains("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        try (Transaction tx = grid(0).transactions().txStart()) {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.containsKeysAsync(set).get();
        } catch (IgniteException e) {
            err = e;
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage().
                contains("Transaction spans operations on atomic cache"));
    }

    /** */
    private class IncEntryProcessor implements EntryProcessor<Integer, Integer, Object> {
        @Override public Object process(MutableEntry<Integer, Integer> entry, Object... objects) throws EntryProcessorException {
            entry.setValue(entry.getValue() + 1);

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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.invoke(1, new IncEntryProcessor());
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage().
                startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);

        assertEquals((Integer) 1, cache.get(1));

        try (Transaction tx = grid(0).transactions().txStart()) {
            cache.invokeAsync(1, new IncEntryProcessor()).get();
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage().
                startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);
        cache.put(2, 1);

        assertEquals((Integer) 1, cache.get(1));
        assertEquals((Integer) 1, cache.get(2));

        try (Transaction tx = grid(0).transactions().txStart()) {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.invokeAll(set, new IncEntryProcessor());
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
            cache.remove(2);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage().
                startsWith("Transaction spans operations on atomic cache"));
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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (isAtomicCacheAllowedInTx)
            cache = cache.withAllowAtomicOpsInTx();

        IgniteException err = null;

        cache.put(1, 1);
        cache.put(2, 1);

        assertEquals((Integer) 1, cache.get(1));
        assertEquals((Integer) 1, cache.get(2));

        try (Transaction tx = grid(0).transactions().txStart()) {
            HashSet<Integer> set = new HashSet<>();

            set.add(1);
            set.add(2);

            cache.invokeAllAsync(set, new IncEntryProcessor()).get();
        } catch (IgniteException e) {
            err = e;
        } finally {
            cache.remove(1);
            cache.remove(2);
        }

        if (isAtomicCacheAllowedInTx)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage().
                startsWith("Transaction spans operations on atomic cache"));
    }
}
