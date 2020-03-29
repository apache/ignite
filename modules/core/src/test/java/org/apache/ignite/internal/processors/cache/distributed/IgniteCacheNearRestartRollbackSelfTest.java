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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCacheNearRestartRollbackSelfTest extends GridCommonAbstractTest {
    /**
     * The number of entries to put to the test cache.
     */
    private static final int ENTRY_COUNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientFailureDetectionTimeout(50000);
        cfg.setCacheConfiguration(cacheConfiguration(igniteInstanceName));

        ((TcpDiscoverySpi)cfg.getDiscoverySpi())
            .setForceServerMode(getTestIgniteInstanceName(3).equals(igniteInstanceName));

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(String igniteInstanceName) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        ccfg.setBackups(1);

        ccfg.setNearConfiguration(new NearCacheConfiguration<>());

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestarts() throws Exception {
        startGrids(3);

        Ignite tester = startClientGrid(3);

        final AtomicLong lastUpdateTs = new AtomicLong(System.currentTimeMillis());

        try {
            Set<Integer> keys = new LinkedHashSet<>();

            for (int i = 0; i < ENTRY_COUNT; i++)
                keys.add(i);

            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (int i = 0; i < 50; i++) {
                        stopGrid(0);

                        startGrid(0);

                        stopGrid(1);

                        startGrid(1);

                        stopGrid(2);

                        startGrid(2);

                        synchronized (lastUpdateTs) {
                            while (System.currentTimeMillis() - lastUpdateTs.get() > 1_000) {
                                info("Will wait for an update operation to finish.");

                                lastUpdateTs.wait(1_000);
                            }
                        }
                    }

                    return null;
                }
            });

            int currVal = 0;
            boolean invoke = false;

            while (!fut.isDone()) {
                try {
                    updateCache(tester, currVal, invoke, false, keys);

                    updateCache(tester, currVal + 1, invoke, true, keys);

                    invoke = !invoke;
                    currVal++;

                    synchronized (lastUpdateTs) {
                        lastUpdateTs.set(System.currentTimeMillis());

                        lastUpdateTs.notifyAll();
                    }
                }
                catch (Throwable e) {
                    log.error("Update failed: " + e, e);

                    throw e;
                }
            }

            fut.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Updates the cache or rollback the update.
     *
     * @param ignite Ignite instance to use.
     * @param newVal the new value to put to the entries
     * @param invoke whether to use invokeAll() or putAll()
     * @param rollback whether to rollback the changes or commit
     * @param keys Collection of keys to update.
     */
    private void updateCache(
        Ignite ignite,
        int newVal,
        boolean invoke,
        boolean rollback,
        Set<Integer> keys
    ) {
        final IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        if (rollback) {
            while (true) {
                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    updateEntries(cache, newVal, invoke, keys);

                    tx.rollback();

                    break;
                }
                catch (CacheException e) {
                    if (e.getCause() instanceof ClusterTopologyException) {
                        ClusterTopologyException topEx = (ClusterTopologyException)e.getCause();

                        topEx.retryReadyFuture().get();
                    }
                    else
                        throw e;
                }
                catch (ClusterTopologyException e) {
                    IgniteFuture<?> fut = e.retryReadyFuture();

                    fut.get();
                }
                catch (TransactionRollbackException ignore) {
                    // Safe to retry right away.
                }
            }
        }
        else
            updateEntries(cache, newVal, invoke, keys);
    }

    /**
     * Update the cache using either invokeAll() or putAll().
     *
     * @param cache the cache
     * @param newVal the new value to put to the entries
     * @param invoke whether to use invokeAll() or putAll()
     * @param keys Keys to update.
     */
    private void updateEntries(
        Cache<Integer, Integer> cache,
        int newVal,
        boolean invoke,
        Set<Integer> keys
    ) {
        if (invoke)
            cache.invokeAll(keys, new IntegerSetValue(newVal));
        else {
            final Map<Integer, Integer> entries = new HashMap<>(ENTRY_COUNT);

            for (final Integer key : keys)
                entries.put(key, newVal);

            cache.putAll(entries);
        }
    }

    /**
     * {@link EntryProcessor} used to update the entry value.
     */
    private static class IntegerSetValue implements EntryProcessor<Integer, Integer, Boolean>, Serializable {
        /** */
        private final int newVal;

        /**
         * @param newVal New value.
         */
        private IntegerSetValue(final int newVal) {
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Integer, Integer> entry, Object... arguments)
            throws EntryProcessorException {
            entry.setValue(newVal);

            return Boolean.TRUE;
        }
    }
}
