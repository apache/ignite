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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.assertions.Assertion;
import org.apache.ignite.testframework.assertions.CacheNodeSafeAssertion;
import org.apache.ignite.testframework.assertions.EventualAssertion;
import org.apache.ignite.testframework.junits.common.GridRollingRestartAbstractTest;
import org.apache.ignite.transactions.Transaction;

/**
 * Test running transactional gets and puts against a running cluster that is undergoing a rolling restart.
 */
public class GridCacheRebalancingTransactionTest extends GridRollingRestartAbstractTest {
    /** The number of entries to put to the test cache. */
    private static final int ENTRY_COUNT = 100;

    /** Test cache name. */
    private static final String CACHE_NAME = "TRANSACTION_TEST_NEAR";

    /** Near Cache Configuration. */
    private static final NearCacheConfiguration<Integer, Integer> NEAR_CACHE_CONFIGURATION = new NearCacheConfiguration<Integer, Integer>()
            .setNearEvictionPolicy(new LruEvictionPolicy<Integer, Integer>(Integer.MAX_VALUE));

    /** The {@link CacheConfiguration} used by this test. */
    @Override protected CacheConfiguration<?, ?> getCacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(true /* machine-safe */, 271))
                .setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.CLOCK)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setRebalanceMode(CacheRebalanceMode.SYNC)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setEvictionPolicy(NEAR_CACHE_CONFIGURATION.getNearEvictionPolicy())
                .setCopyOnRead(false)
                .setNearConfiguration(NEAR_CACHE_CONFIGURATION);
    }

    /** Keys of entries being read and updated. */
    private static final Set<Integer> KEYS;
    static {
        final Set<Integer> keys = new LinkedHashSet<>();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            keys.add(i);
        }
        KEYS = Collections.unmodifiableSet(keys);
    }

    /** The number of transactions since the last node restart. */
    private volatile int transactionCount;

    /** {@inheritDoc} */
    @Override public int serverCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public int getMaxRestarts() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public IgnitePredicate<Ignite> getRestartCheck() {
        return new IgnitePredicate<Ignite>() {
            @Override
            public boolean apply(Ignite ignite) {
                return serverCount() <= grid(0).cluster().forServers().nodes().size()
                        && GridCacheRebalancingTransactionTest.this.transactionCount > 0;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Assertion getRestartAssertion() {
        return new EventualAssertion(5000, new CacheNodeSafeAssertion(grid(0), CACHE_NAME));
    }

    /**
     * This test performs a transactional update and checksum of the cache, as well as validates that the cache is
     * not changed when a transaction is rolled back.
     */
    public void testRollingRestartTransaction()
            throws InterruptedException {

        Cache<Integer, Integer> cache = (Cache<Integer, Integer>) grid(0).getOrCreateCache(getCacheConfiguration());

        boolean invoke = true;
        int currentValue = 0;
        int checksumFailures = 0;
        int transactionCount = 0;
        Map<Integer, Integer> lastChecksum = null;
        do {
            updateCache(cache, currentValue, invoke, false);
            Map<Integer, Integer> checksum = checksumCache(cache, currentValue, lastChecksum);
            if (checksum == null) {
                grid(0).log().info(
                        "Checksum failure after commit (expected " + currentValue + ", invoke " + invoke + ").");
                checksumFailures++;
            }
            else {
                lastChecksum = checksum;
                updateCache(cache, currentValue + 1, invoke, true);
                checksum = checksumCache(cache, currentValue, lastChecksum);
                if (checksum == null) {
                    checksumFailures++;
                    grid(0).log().info(
                            "Checksum failure after rollback (expected " + currentValue + ", invoke " + invoke + ").");
                }
                else {
                    lastChecksum = checksum;
                }
            }

            invoke = !invoke;
            currentValue++;
            transactionCount++;
            this.transactionCount = transactionCount;
        } while (this.rollingRestartThread.isAlive());

        grid(0).log().info("Total checksum failures during the test were: " + checksumFailures);
        grid(0).log().info("Total transactions during the test were: " + transactionCount);
        grid(0).log().info("Total node restarts during the test were: " + this.rollingRestartThread.getRestartTotal());

        assertEquals(getMaxRestarts(), this.rollingRestartThread.getRestartTotal());
        assertEquals(0, checksumFailures);
    }

    /**
     * Updates the cache and either commit or rollback the update.
     *
     * @param cache the cache
     * @param newValue the new value to update the entries with
     * @param invoke whether to use invokeAll() or putAll()
     * @param rollback whether to rollback the changes or commit
     */
    private void updateCache(final Cache<Integer, Integer> cache, final int newValue, final boolean invoke, final boolean rollback) {
        final TransactionConfiguration txConfig = new TransactionConfiguration();
        transact(new TransactionalTask<Object>() {
            @Override public Object run(Transaction tx) {
                if (invoke) {
                    for (EntryProcessorResult<Boolean> result : cache.invokeAll(KEYS, new IntegerSetValue(newValue)).values()) {
                        if (!Boolean.TRUE.equals(result.get())) {
                            throw new IllegalStateException();
                        }
                    }
                }
                else {
                    Map<Integer, Integer> entries = new HashMap<>(ENTRY_COUNT);
                    for (Integer key : KEYS) {
                        entries.put(key, newValue);
                    }
                    cache.putAll(entries);
                }

                if (rollback) {
                    tx.rollback();
                }
                else {
                    tx.commit();
                }
                return null;
            }
        }, txConfig);
    }

    /**
     * Validate that all entries in the cache have the same value.
     *
     * @param cache The cache.
     * @param currentValue The value to validate.
     * @param lastChecksum The last validated entries returned by this method.
     *
     * @return A map of all validated entries or null if there was a checksum error.
     */
    private Map<Integer, Integer> checksumCache(final Cache<Integer, Integer> cache,
                                                final int currentValue,
                                                final Map<Integer, Integer> lastChecksum) {
        TransactionConfiguration txConfig = new TransactionConfiguration();
        return transact(new TransactionalTask<Map<Integer, Integer>>() {
            @Override public Map<Integer, Integer> run(Transaction tx) {
                Map<Integer, Integer> map = cache.getAll(KEYS);
                for (int value : map.values()) {
                    if (value != currentValue) {
                        grid(0).log().info(
                                "Failed testing that all entries have a value of: " + currentValue + "\n\nCurrent: "
                                        + map + "\n\nLast: " + lastChecksum);
                        return null;
                    }
                }
                return map;
            }
        }, txConfig);
    }

    /**
     * Execute the {@link TransactionalTask} and return the result obtained.
     *
     * @param task The task to execute.
     * @param config The transaction configuration.
     * @param <T> The type of result.
     *
     * @return The result of executing the transaction.
     * @throws RuntimeException If there are errors.
     */
    private <T> T transact(TransactionalTask<T> task, TransactionConfiguration config) {
        if (task == null) {
            throw new IllegalArgumentException("Missing TransactionalTask");
        }
        if (config == null) {
            throw new IllegalArgumentException("Missing TransactionConfiguration");
        }
        Throwable e = null;
        for (int i = 0, c = 20; i <= c; ++i) {
            try (Transaction tx = startTransaction(config)) {
                T t = task.run(tx);
                if (!tx.isRollbackOnly()) {
                    tx.commit();
                }
                return t;
            }
            catch (final Throwable ee) {
                e = ee;
            }
        }
        throw new RuntimeException("Failed to commit transaction", e);
    }

    /**
     * Helper method to start / continue an existing {@link Transaction}.
     *
     * @param config The transaction configuration to use to determine the timeout.
     *
     * @return The existing Transaction or a newly started Transaction.
     * @throws IllegalStateException If there are exceptions.
     */
    private Transaction startTransaction(TransactionConfiguration config) {
        Ignite ignite = grid(0);
        Transaction txThread = ignite.transactions().tx();

        // We do not support nested transactions. If one has already been started, return the existing transaction
        if (txThread == null) {
            try {
                Transaction tx = ignite.transactions().txStart();
                tx.timeout(config.getDefaultTxTimeout());
                return tx;
            }
            catch (RuntimeException e) {
                throw new IllegalStateException(e);
            }
        }
        else {
            return txThread;
        }
    }

    /**
     * Interface to represent the execution of a transaction that returns a result.
     *
     * @param <T> The results of executing the transaction.
     */
    interface TransactionalTask<T> {
        /**
         * Run the task.
         *
         * @param tx The current transaction.
         * @return The result from running the task.
         */
        T run(Transaction tx);
    }

    /**
     * {@link EntryProcessor} used to update an entry with a specified integer value.
     */
    private static class IntegerSetValue
            implements EntryProcessor<Integer, Integer, Boolean>, Serializable {
        /** The value. */
        private final int newValue;

        /**
         * Create a new {@link IntegerSetValue} processor that will update a targeted entry with the specified value.
         *
         * @param newValue The new value.
         */
        public IntegerSetValue(int newValue) {
            this.newValue = newValue;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Integer, Integer> entry, Object... args) {
            entry.setValue(this.newValue);
            return Boolean.TRUE;
        }
    }
}
