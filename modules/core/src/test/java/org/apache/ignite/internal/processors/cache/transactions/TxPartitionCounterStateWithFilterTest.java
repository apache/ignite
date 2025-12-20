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

import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test if NOOP tx operation skips incrementing update counter for entry partition.
 */
@ParameterizedClass(name = "cacheMode={0}, backups={1}, sameTx={2}")
@MethodSource("allTypesArgs")
public class TxPartitionCounterStateWithFilterTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 4;

    /** */
    @Parameter(0)
    public CacheMode cacheMode;

    /** */
    @Parameter(1)
    public int backups;

    /** */
    @Parameter(2)
    public boolean sameTx;

    /** */
    private static Collection<Arguments> allTypesArgs() {
        return List.of(
            Arguments.of(REPLICATED, -1, false),
            Arguments.of(REPLICATED, -1, true),
            Arguments.of(PARTITIONED, 2, false),
            Arguments.of(PARTITIONED, 2, true),
            Arguments.of(PARTITIONED, 1, false),
            Arguments.of(PARTITIONED, 1, true),
            Arguments.of(PARTITIONED, 0, false),
            Arguments.of(PARTITIONED, 0, true)
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES - 1);

        startClientGrid(NODES - 1);
    }

    /** */
    @Test
    public void testAssignCountersInTxWithFilter() {
        for (Ignite ig : G.allGrids()) {
            for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    try {
                        ignite(0).createCache(cacheConfiguration(cacheMode, backups, CacheAtomicityMode.TRANSACTIONAL));

                        awaitCacheOnClient(ig, DEFAULT_CACHE_NAME);

                        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

                        assertNotNull(cache);

                        int partId = 0;

                        List<Integer> keys = partitionKeys(cache, partId, 2, 0);

                        int key = keys.get(0), val = 0;

                        if (!sameTx)
                            cache.put(key, val);

                        try (Transaction tx = ig.transactions().txStart(concurrency, isolation)) {
                            if (sameTx)
                                cache.put(key, val);

                            Object prev = cache.getAndPutIfAbsent(key, val + 1);

                            assertNotNull(prev);

                            cache.put(keys.get(1), val);

                            tx.commit();
                        }

                        assertEquals(Integer.valueOf(val), cache.get(key));
                        assertEquals(Integer.valueOf(val), cache.get(keys.get(1)));

                        for (Ignite ignite : G.allGrids()) {
                            if (ignite.configuration().isClientMode())
                                continue;

                            PartitionUpdateCounter cntr = counter(partId, ignite.name());

                            if (cntr != null)
                                assertEquals("Expecting counter for node=" + ignite.name(), 2, cntr.get());
                        }
                    }
                    finally {
                        ignite(0).destroyCache(DEFAULT_CACHE_NAME);
                    }
                }
            }
        }
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }
}
