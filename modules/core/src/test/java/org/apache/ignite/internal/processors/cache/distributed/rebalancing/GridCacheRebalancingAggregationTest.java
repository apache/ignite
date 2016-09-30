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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.assertions.Assertion;
import org.apache.ignite.testframework.assertions.CacheNodeSafeAssertion;
import org.apache.ignite.testframework.junits.common.GridRollingRestartAbstractTest;
import org.apache.ignite.transactions.Transaction;

/**
 * Test running a continuous aggregate sum against a running cluster that is undergoing a rolling restart.
 */
public class GridCacheRebalancingAggregationTest extends GridRollingRestartAbstractTest {
    /** The number of entries to put to the test cache. */
    private static final int ENTRY_COUNT = 100;

    /** The maximum number of times to restart nodes in the cluster. */
    private static final int MAX_RESTARTS = 5;

    /** The key of the cache entry that contains the current upper range of the aggregation. */
    private static final int CEILING_KEY = Integer.MAX_VALUE;

    /** Test cache name. */
    private static final String CACHE_NAME = "AGGREGATION_TEST";

    /** Keys of entries under test. */
    private static final Set<Integer> KEYS;

    /** Initialize the set of keys for the test. */
    static {
        final Set<Integer> keys = new LinkedHashSet<>();

        for (int i = 0; i < ENTRY_COUNT; i++)
            keys.add(i);

        KEYS = Collections.unmodifiableSet(keys);
    }

    /** Thread responsible for writing to the cache. */
    private UpdateCacheThread updateThread;

    /** The number of aggregations since the last node restart. */
    private volatile int aggregationCount;

    /** Total number of times we updated the cache. */
    private volatile int totalUpdateCount;

    /** {@inheritDoc} */
    @Override public int serverCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public int getMaxRestarts() {
        return MAX_RESTARTS;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 1000 * 60 * 5;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration<?, ?> getCacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>(CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1);
    }

    /** {@inheritDoc} */
    @Override public IgnitePredicate<Ignite> getRestartCheck() {
        return new IgnitePredicate<Ignite>() {
            @Override public boolean apply(final Ignite ignite) {
                return serverCount() >= ignite.cluster().forServers().nodes().size()
                        && GridCacheRebalancingAggregationTest.this.aggregationCount > 0;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Assertion getRestartAssertion() {
        return new CacheNodeSafeAssertion(grid(0), CACHE_NAME);
    }

    /**
     * This test has three threads; the writer thread updates a monotonically increasing set of values for the keys,
     * while the reader thread is performing a checksum on the contents of the cache while the restart thread is
     * killing nodes. This test will fail if we lose any data.
     */
    public void testRollingRestartAggregation()
        throws InterruptedException {

        startUpdateThread();

        RollingRestartThread restartThread = super.rollingRestartThread;

        int totalAggregationCount = 0;
        int checksumFailures = 0;

        do {
            if (!checksumCache())
                checksumFailures++;

            totalAggregationCount++;

            this.aggregationCount++;
        }
        while (restartThread.isAlive());

        grid(0).log().info("Total successful aggregations during the test were: " + totalAggregationCount);
        grid(0).log().info("Total successful updates during the test were: " + this.totalUpdateCount);
        grid(0).log().info("Total node restarts during the test were: " + restartThread.getRestartTotal());

        this.updateThread.join();

        assertEquals(0, checksumFailures);
        assertEquals(getMaxRestarts(), restartThread.getRestartTotal());
        assertFalse(this.updateThread.isException());
    }

    /**
     * Create and start the {@link UpdateCacheThread}.
     */
    private void startUpdateThread() {
        UpdateCacheThread updateThread = new UpdateCacheThread();

        this.updateThread = updateThread;

        updateThread.start();
    }

    /**
     * Check that the values in the cache are equal to the sum of values between the floor and ceiling (key in the cache
     * signifying the highest value that has been written).
     *
     * @return {@code True} if the cache values sum to the expected value
     */
    private boolean checksumCache() {
        final Ignite ignite = grid(0);

        Cache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

        Throwable e = null;

        for (int i = 0, c = 20; i <= c; ++i) {
            try (Transaction tx = ignite.transactions().txStart()) {
                tx.timeout(2 * 60 * 1000L);

                Integer ceiling = cache.get(CEILING_KEY);

                if (ceiling == null)
                    return Boolean.TRUE;

                int floor = ceiling - ENTRY_COUNT + 1;

                Map<Integer, Integer> results = cache.getAll(KEYS);

                tx.commit();

                int sum = 0;

                for (Integer j : results.values())
                    sum += j;

                if (sum == calculateSum(floor, ceiling))
                    return Boolean.TRUE;

                grid(0).log().info("Failed testing that sum of " + sum + " equals sum(" + floor + "," + ceiling + ")");

                return Boolean.FALSE;
            }
            catch (Throwable ee) {
                e = ee;
            }
        }

        throw new RuntimeException("Failed to commit transaction after " + 20 + " retries", e);
    }

    /**
     * Update the values in the cache.
     */
    private void updateCache() {
        final Ignite ignite = grid(0);

        Cache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

        Throwable e = null;

        for (int i = 0, c = 20; i <= c; ++i) {
            try (Transaction tx = ignite.transactions().txStart()) {
                tx.timeout(2 * 60 * 1000L);

                Integer curCeiling = cache.get(CEILING_KEY);

                int newCeiling = curCeiling == null ? ENTRY_COUNT - 1 : curCeiling + ENTRY_COUNT;
                int value = curCeiling == null ? 0 : curCeiling + 1;

                Map<Integer, Integer> entries = new TreeMap<>();

                for (Integer key : KEYS)
                    entries.put(key, value++);

                cache.putAll(entries);
                cache.put(CEILING_KEY, newCeiling);

                GridCacheRebalancingAggregationTest.this.totalUpdateCount++;

                if (!tx.isRollbackOnly())
                    tx.commit();

                return;
            }
            catch (Throwable ee) {
                e = ee;
            }
        }

        throw new RuntimeException("Failed to commit transaction after " + 20 + " retries", e);
    }

    /**
     * Calculate the sums of a range of values from floor to ceiling. This expects 0 gaps in the range.
     *
     * @param f The floor of the range.
     * @param c The ceiling of the range.
     *
     * @return The sum.
     */
    private int calculateSum(long f, long c) {
        return (int) ((c * (c + 1)) / 2 - ((f - 1) * ((f - 1) + 1)) / 2);
    }

    /**
     * Update cache thread.
     */
    private class UpdateCacheThread extends Thread {
        /** Flag to indicate if an exception occurred during update. */
        private boolean exception = false;

        /**
         * Default Constructor sets this thread to run as a daemon.
         */
        UpdateCacheThread() {
            setDaemon(true);
            setName(UpdateCacheThread.class.getSimpleName());
        }

        /**
         * Determine if the thread exited due to an exception.
         * @return {@code True} if the thread encountered an exception
         */
        boolean isException() {
            return this.exception;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            RollingRestartThread restartThread =  GridCacheRebalancingAggregationTest.this.rollingRestartThread;

            try {
                do {
                    GridCacheRebalancingAggregationTest.this.updateCache();
                }
                while (restartThread.isAlive());
            }
            catch (Throwable t) {
                log.error("Update thread exiting due to failure", t);
                this.exception = true;
            }
        }
    }
}
