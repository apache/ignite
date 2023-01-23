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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ALLOW_ATOMIC_OPS_IN_TX;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CachePeekMode.ALL;

/**
 * Checks how atomic cache operations work within a transaction.
 */
public class AtomicOperationsInTxTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setName(DEFAULT_CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid(0);
    }

    /**
     * Tests whether enabling atomic cache operations within transactions is allowed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_ATOMIC_OPS_IN_TX, value = "false")
    public void testEnablingAtomicOperationDuringTransaction() {
        GridTestUtils.assertThrows(log, (Callable<IgniteCache>)() -> {
            try (Transaction tx = grid(0).transactions().txStart()) {
                return grid(0).cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();
            }
        },
            IllegalStateException.class,
            "Enabling atomic operations during active transaction is not allowed. Enable atomic operations before " +
                    "transaction start."
        );
    }

    /**
     * Tests whether allowing atomic cache operations before transaction starts does not cause exceptions.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_ATOMIC_OPS_IN_TX, value = "true")
    public void testAllowAtomicOperationsBeforeTransactionStarts() {
        try (Transaction tx = grid(0).transactions().txStart()) {
            grid(0).cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();
        }
    }

    /**
     * Tests that atomic cache operations within transactions are allowed if the system property
     * {@link IgniteSystemProperties#IGNITE_ALLOW_ATOMIC_OPS_IN_TX IGNITE_ALLOW_ATOMIC_OPS_IN_TX} is not changed from
     * the default {@code false} and the user explicitly allows atomic operations in
     * transactions via {@link IgniteCache#withAllowAtomicOpsInTx() withAllowAtomicOpsInTx()} before transactions start.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_ATOMIC_OPS_IN_TX, value = "false")
    public void testSetOfAtomicOperationsWithinTransactionsCheckFalseSystemPropertyFalse() {
        checkOperations(false, false);
        checkOperations(true, false);
    }

    /**
     * Tests that operations with atomic cache within transactions are allowed if the system property
     * {@link IgniteSystemProperties#IGNITE_ALLOW_ATOMIC_OPS_IN_TX IGNITE_ALLOW_ATOMIC_OPS_IN_TX} is changed to
     * {@code true} before transaction start and the user explicitly allows atomic operations in transactions via
     * {@link IgniteCache#withAllowAtomicOpsInTx() withAllowAtomicOpsInTx()} before transactions start.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_ATOMIC_OPS_IN_TX, value = "true")
    public void testSetOfAtomicOperationsWithinTransactionsCheckFalseSystemPropertyTrue() {
        checkOperations(false, true);
        checkOperations(true, true);
    }

    /**
     * @param withAllowAtomicOpsInTx If true - atomic cache operations within transactions are explicitly allowed by the user.
     * @param allowbyProperty If true - atomic cache operations within transactions are allowed by the system property.
     * @throws IgniteException If failed.
     */
    private void checkOperations(boolean withAllowAtomicOpsInTx, boolean allowbyProperty) {
        HashMap<Integer, Integer> map = new HashMap<>();

        map.put(1, 1);
        map.put(2, 1);

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.put(1, 1));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.putAsync(1, 1).get());

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.putAll(map));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.putAllAsync(map).get());

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.putIfAbsent(1, 1));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.putIfAbsentAsync(1, 1).get());

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.get(1));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.getAll(map.keySet()));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.getAllAsync(map.keySet()).get());

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.getAndPut(1, 2));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.getAndPutAsync(1, 2).get());

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.getAndPutIfAbsent(1, 2));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.getAndPutIfAbsentAsync(1, 2).get());

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.getAndRemove(1));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.getAndRemoveAsync(1));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.getAndReplace(1, 2));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.getAndReplaceAsync(1, 2).get());

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.remove(1, 1));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.removeAsync(1, 1).get());

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.removeAll(map.keySet()));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.removeAllAsync(map.keySet()).get());

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.containsKey(1));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.containsKeyAsync(1).get());

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.containsKeys(map.keySet()));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.containsKeysAsync(map.keySet()).get());

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.invoke(1, new SetEntryProcessor()));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.invokeAsync(1, new SetEntryProcessor()).get());

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.invokeAll(map.keySet(), new SetEntryProcessor()));

        checkOperation(withAllowAtomicOpsInTx, allowbyProperty, cache -> cache.invokeAllAsync(map.keySet(),
            new SetEntryProcessor()).get());

        checkLock(withAllowAtomicOpsInTx, allowbyProperty);
    }

    /**
     * @param withAllowAtomicOpsInTx If true - atomic cache operations within transactions are explicitly allowed by the user.
     * @param allowbyProperty If true - atomic cache operations within transactions are allowed by the system property.
     * Otherwise - it should throw exception.
     * @param op Operation.
     */
    private void checkOperation(boolean withAllowAtomicOpsInTx, boolean allowbyProperty, Consumer<IgniteCache<Integer, Integer>> op) {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        if (withAllowAtomicOpsInTx)
            cache = cache.withAllowAtomicOpsInTx();

        cache.clear();

        assertEquals(0, cache.size(ALL));

        IgniteException err = null;

        try (Transaction tx = grid(0).transactions().txStart()) {
            op.accept(cache);
        }
        catch (IgniteException e) {
            err = e;
        }

        if (withAllowAtomicOpsInTx)
            assertNull(err);
        else {
            if (allowbyProperty)
                assertNull(err);
            else
                assertTrue(err != null && err.getMessage().startsWith("Transaction spans operations on atomic cache"));
        }
    }

    /**
     * @param withAllowAtomicOpsInTx If true - atomic cache operations within transactions are explicitly allowed by the user.
     * @param allowByProperty If true - atomic cache operations within transactions are allowed by the system property
     * {@link IgniteSystemProperties#IGNITE_ALLOW_ATOMIC_OPS_IN_TX IGNITE_ALLOW_ATOMIC_OPS_IN_TX}.
     */
    private void checkLock(boolean withAllowAtomicOpsInTx, boolean allowByProperty) {
        IgniteCache<Integer, Integer> cache;
        Class<? extends Throwable> eCls;
        String eMsg;

        if (withAllowAtomicOpsInTx || allowByProperty) {
            cache = grid(0).cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();
            eCls = CacheException.class;
            eMsg = "Explicit lock can't be acquired within a transaction.";
        }
        else {
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
