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
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
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
     * @throws Exception If failed.
     */
    public void testEnablingAtomicOperationDuringTransaction() throws Exception {
        GridTestUtils.assertThrows(log, (Callable<IgniteCache>)() -> {
            try (Transaction tx = grid(0).transactions().txStart()) {
                return grid(0).cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();
            }
        },
            IllegalStateException.class,
            "Enabling atomic operations during active transaction is not allowed."
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testAllowedAtomicOperations() throws Exception {
        checkOperations(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNotAllowedAtomicOperations() throws Exception {
        checkOperations(false);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     */
    private void checkOperations(boolean isAtomicCacheAllowedInTx) {
        HashMap<Integer, Integer> map = new HashMap<>();

        map.put(1, 1);
        map.put(2, 1);

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.put(1, 1));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.putAsync(1, 1).get());

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.putAll(map));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.putAllAsync(map).get());

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.putIfAbsent(1, 1));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.putIfAbsentAsync(1, 1).get());

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.get(1));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAll(map.keySet()));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAllAsync(map.keySet()).get());

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndPut(1, 2));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndPutAsync(1, 2).get());

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndPutIfAbsent(1, 2));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndPutIfAbsentAsync(1, 2).get());

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndRemove(1));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndRemoveAsync(1));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndReplace(1, 2));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.getAndReplaceAsync(1, 2).get());

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.remove(1, 1));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.removeAsync(1, 1).get());

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.removeAll(map.keySet()));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.removeAllAsync(map.keySet()).get());

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.containsKey(1));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.containsKeyAsync(1).get());

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.containsKeys(map.keySet()));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.containsKeysAsync(map.keySet()).get());

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.invoke(1, new SetEntryProcessor()));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.invokeAsync(1, new SetEntryProcessor()).get());

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.invokeAll(map.keySet(), new SetEntryProcessor()));

        checkOperation(isAtomicCacheAllowedInTx, cache -> cache.invokeAllAsync(map.keySet(),
            new SetEntryProcessor()).get());

        checkLock(isAtomicCacheAllowedInTx);
    }

    /**
     * @param isAtomicCacheAllowedInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     * @param op Operation.
     */
    private void checkOperation(boolean isAtomicCacheAllowedInTx, Consumer<IgniteCache<Integer, Integer>> op) {
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
    private void checkLock(boolean isAtomicCacheAllowedInTx) {
        IgniteCache<Integer, Integer> cache;
        Class<? extends Throwable> eCls;
        String eMsg;

        if (isAtomicCacheAllowedInTx) {
            cache = grid(0).cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();
            eCls = CacheException.class;
            eMsg = "Explicit lock can't be acquired within a transaction.";
        } else {
            cache = grid(0).cache(DEFAULT_CACHE_NAME);
            eCls = IgniteException.class;
            eMsg = "Transaction spans operations on atomic cache";
        }

        Lock lock = cache.lock(1);

        GridTestUtils.assertThrows(log, (Callable<Void>)() -> {
            try (Transaction tx = grid(0).transactions().txStart()) {
                lock.lock();
            }

            return null;
        }, eCls, eMsg);
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
}
