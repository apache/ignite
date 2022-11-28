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
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopGrid(0);
    }

    /**
     * Tests whether enabling atomic cache operations within transactions is allowed.
     * Since 2.15.0 the default behaviour is set to {@code false}.
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_ATOMIC_OPS_IN_TX, value = "false")
    public void testEnablingAtomicOperationDuringTransaction() throws Exception {
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
     * Since 2.15.0 the default behaviour is set to {@code false}.
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_ATOMIC_OPS_IN_TX, value = "true")
    public void testAllowAtomicOperationsBeforeTransactionStarts() throws Exception {
        try (Transaction tx = grid(0).transactions().txStart()) {
            grid(0).cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();
        }
    }

    /**
     * Tests that operations with atomic cache within transactions are allowed if the system property
     * {@link IgniteSystemProperties#IGNITE_ALLOW_ATOMIC_OPS_IN_TX IGNITE_ALLOW_ATOMIC_OPS_IN_TX} is not changed from
     * the default {@code false} and the user explicitly allows atomic operations in
     * transactions via {@link IgniteCache#withAllowAtomicOpsInTx() withAllowAtomicOpsInTx()} before transactions start.
     * Since 2.15.0 the default behaviour is set to {@code false}.
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_ATOMIC_OPS_IN_TX, value = "false")
    public void testSetOfNonAtomicOperationsWithinTransactions() throws Exception {
        checkOperations(true);
    }

    /**
     * Tests that operations with atomic cache within transactions are allowed if the system property
     * {@link IgniteSystemProperties#IGNITE_ALLOW_ATOMIC_OPS_IN_TX IGNITE_ALLOW_ATOMIC_OPS_IN_TX} is changed to
     * {@code true} before transaction start and the user explicitly allows atomic operations in transactions via
     * {@link IgniteCache#withAllowAtomicOpsInTx() withAllowAtomicOpsInTx()} before transactions start.
     * Since 2.15.0 the default behaviour is set to {@code false}.
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_ATOMIC_OPS_IN_TX, value = "true")
    public void testSetOfAtomicOperationsWithinTransactions() throws Exception {
        checkOperations(true);
    }

    /**
     * Tests that atomic cache operations within transactions are forbidden if system property
     * {@link IgniteSystemProperties#IGNITE_ALLOW_ATOMIC_OPS_IN_TX IGNITE_ALLOW_ATOMIC_OPS_IN_TX}
     * is changed to {@code true} and the user does not explicitly allow atomic operations in transactions via
     * {@link IgniteCache#withAllowAtomicOpsInTx() withAllowAtomicOpsInTx()} before transactions start.
     * Since 2.15.0 the default behaviour is set to {@code false}.
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_ATOMIC_OPS_IN_TX, value = "false")
    public void testSetOfAtomicOperationsWithinTransactionsCheckFalseSystemPropertyFalse() throws Exception {
        checkOperations(false);
    }

    /**
     * Tests that atomic cache operations within transactions are forbidden if system property
     * {@link IgniteSystemProperties#IGNITE_ALLOW_ATOMIC_OPS_IN_TX IGNITE_ALLOW_ATOMIC_OPS_IN_TX}
     * is changed to {@code true} and the user does not explicitly allow atomic operations in transactions via
     * {@link IgniteCache#withAllowAtomicOpsInTx() withAllowAtomicOpsInTx()} before transactions start.
     * Since 2.15.0 the default behaviour is set to {@code false}.
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_ATOMIC_OPS_IN_TX, value = "true")
    public void testSetOfAtomicOperationsWithinTransactionsCheckFalseSystemPropertyTrue() throws Exception {
        checkOperations(false);
    }

    /**
     * @throws IgniteException If failed.
     */
    private void checkOperations(boolean withAllowAtomicOpsInTx) {
        HashMap<Integer, Integer> map = new HashMap<>();

        map.put(1, 1);
        map.put(2, 1);

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.put(1, 1));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.putAsync(1, 1).get());

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.putAll(map));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.putAllAsync(map).get());

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.putIfAbsent(1, 1));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.putIfAbsentAsync(1, 1).get());

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.get(1));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.getAll(map.keySet()));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.getAllAsync(map.keySet()).get());

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.getAndPut(1, 2));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.getAndPutAsync(1, 2).get());

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.getAndPutIfAbsent(1, 2));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.getAndPutIfAbsentAsync(1, 2).get());

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.getAndRemove(1));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.getAndRemoveAsync(1));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.getAndReplace(1, 2));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.getAndReplaceAsync(1, 2).get());

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.remove(1, 1));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.removeAsync(1, 1).get());

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.removeAll(map.keySet()));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.removeAllAsync(map.keySet()).get());

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.containsKey(1));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.containsKeyAsync(1).get());

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.containsKeys(map.keySet()));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.containsKeysAsync(map.keySet()).get());

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.invoke(1, new SetEntryProcessor()));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.invokeAsync(1, new SetEntryProcessor()).get());

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.invokeAll(map.keySet(), new SetEntryProcessor()));

        checkOperation(withAllowAtomicOpsInTx, cache -> cache.invokeAllAsync(map.keySet(),
            new SetEntryProcessor()).get());
    }

    /**
     * @param withAllowAtomicOpsInTx If true - atomic operation allowed.
     * Otherwise - it should throw exception.
     * @param op Operation.
     */
    private void checkOperation(boolean withAllowAtomicOpsInTx, Consumer<IgniteCache<Integer, Integer>> op) {
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
        else if (!withAllowAtomicOpsInTx && err == null)
            assertNull(err);
        else
            assertTrue(err != null && err.getMessage()
                .startsWith("Transaction spans operations on atomic cache"));

        checkLock(withAllowAtomicOpsInTx, err);
    }

    /**
     * @param withAllowAtomicOpsInTx If true - atomic operation are explicitly allowed.
     * @param err If exception is thrown.
     */
    private void checkLock(boolean withAllowAtomicOpsInTx, Exception err) {
        IgniteCache<Integer, Integer> cache;
        Class<? extends Throwable> eCls;
        String eMsg;

        if (withAllowAtomicOpsInTx) {
            cache = grid(0).cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();
            eCls = CacheException.class;
            eMsg = "Explicit lock can't be acquired within a transaction.";
        }
        else if (!withAllowAtomicOpsInTx && err == null) {
            cache = grid(0).cache(DEFAULT_CACHE_NAME);
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
