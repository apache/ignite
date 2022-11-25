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
import java.util.function.Consumer;
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
            IgniteException.class,
            "Transaction spans operations on atomic cache (don't use atomic cache inside transaction or set up" +
            " flag by cache.allowedAtomicOpsInTx())."
        );
    }

    /**
     * Tests whether allowing atomic cache operations before transaction starts does not cause exceptions.
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
     * Tests that operations with atomic cache within transactions are not allowed if the system property
     * {@link IgniteSystemProperties#IGNITE_ALLOW_ATOMIC_OPS_IN_TX IGNITE_ALLOW_ATOMIC_OPS_IN_TX} is not changed from
     * the default {@code false} before transaction start. It is the default behaviour since 2.15.0.
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_ATOMIC_OPS_IN_TX, value = "false")
    public void testSetOfNonAtomicOperationsWithinTransactions() throws Exception {
        GridTestUtils.assertThrows(
            log,
            () -> checkOperations(),
            IgniteException.class,
            "Transaction spans operations on atomic cache (don't use atomic cache inside transaction or set up" +
            " flag by cache.allowedAtomicOpsInTx())."
        );
    }

    /**
     * Tests that operations with atomic cache within transactions are allowed if the system property
     * {@link IgniteSystemProperties#IGNITE_ALLOW_ATOMIC_OPS_IN_TX IGNITE_ALLOW_ATOMIC_OPS_IN_TX} is changed to
     * {@code true} before transaction start. Since 2.15.0 the default behaviour is set to {@code false}.
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_ALLOW_ATOMIC_OPS_IN_TX, value = "true")
    public void testSetOfAtomicOperationsWithinTransactions() throws Exception {
        checkOperations();
    }

    /**
     * @throws IgniteException If failed.
     */
    private void checkOperations() throws IgniteException {
        HashMap<Integer, Integer> map = new HashMap<>();

        map.put(1, 1);
        map.put(2, 1);

        checkOperation(cache -> cache.put(1, 1));

        checkOperation(cache -> cache.putAsync(1, 1).get());

        checkOperation(cache -> cache.putAll(map));

        checkOperation(cache -> cache.putAllAsync(map).get());

        checkOperation(cache -> cache.putIfAbsent(1, 1));

        checkOperation(cache -> cache.putIfAbsentAsync(1, 1).get());

        checkOperation(cache -> cache.get(1));

        checkOperation(cache -> cache.getAll(map.keySet()));

        checkOperation(cache -> cache.getAllAsync(map.keySet()).get());

        checkOperation(cache -> cache.getAndPut(1, 2));

        checkOperation(cache -> cache.getAndPutAsync(1, 2).get());

        checkOperation(cache -> cache.getAndPutIfAbsent(1, 2));

        checkOperation(cache -> cache.getAndPutIfAbsentAsync(1, 2).get());

        checkOperation(cache -> cache.getAndRemove(1));

        checkOperation(cache -> cache.getAndRemoveAsync(1));

        checkOperation(cache -> cache.getAndReplace(1, 2));

        checkOperation(cache -> cache.getAndReplaceAsync(1, 2).get());

        checkOperation(cache -> cache.remove(1, 1));

        checkOperation(cache -> cache.removeAsync(1, 1).get());

        checkOperation(cache -> cache.removeAll(map.keySet()));

        checkOperation(cache -> cache.removeAllAsync(map.keySet()).get());

        checkOperation(cache -> cache.containsKey(1));

        checkOperation(cache -> cache.containsKeyAsync(1).get());

        checkOperation(cache -> cache.containsKeys(map.keySet()));

        checkOperation(cache -> cache.containsKeysAsync(map.keySet()).get());

        checkOperation(cache -> cache.invoke(1, new SetEntryProcessor()));

        checkOperation(cache -> cache.invokeAsync(1, new SetEntryProcessor()).get());

        checkOperation(cache -> cache.invokeAll(map.keySet(), new SetEntryProcessor()));

        checkOperation(cache -> cache.invokeAllAsync(map.keySet(),
            new SetEntryProcessor()).get());
    }

    /**
     * @param op Operation.
     * @throws IgniteException If failed.
     */
    private void checkOperation(Consumer<IgniteCache<Integer, Integer>> op) throws IgniteException {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = grid(0).transactions().txStart()) {
            grid(0).cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();
            op.accept(cache);
        }
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
