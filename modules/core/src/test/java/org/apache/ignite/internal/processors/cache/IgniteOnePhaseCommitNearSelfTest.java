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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import javax.cache.*;

/**
 * Checks one-phase commit scenarios.
 */
public class IgniteOnePhaseCommitNearSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 4;

    /** */
    private int backups = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(String gridName) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setBackups(backups);
        ccfg.setDistributionMode(CacheDistributionMode.NEAR_PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.PARTITIONED);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnePhaseCommitFromNearNode() throws Exception {
        backups = 1;

        startGrids(GRID_CNT);

        try {
            awaitPartitionMapExchange();

            int key = generateNearKey();

            IgniteCache<Object, Object> cache = ignite(0).jcache(null);

            checkKey(ignite(0).transactions(), cache, key);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param transactions Transactions instance.
     * @param cache Cache instance.
     * @param key Key.
     */
    private void checkKey(IgniteTransactions transactions, Cache<Object, Object> cache, int key) throws Exception {
        cache.put(key, key);

        finalCheck(key);

        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                info("Checking transaction [isolation=" + isolation + ", concurrency=" + concurrency + ']');

                try (Transaction tx = transactions.txStart(concurrency, isolation)) {
                    cache.put(key, isolation + "-" + concurrency);

                    tx.commit();
                }

                finalCheck(key);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void finalCheck(final int key) throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    for (int i = 0; i < GRID_CNT; i++) {
                        GridCacheAdapter<Object, Object> cache = ((IgniteKernal)ignite(i)).internalCache();

                        GridCacheEntryEx<Object, Object> entry = cache.peekEx(key);

                        if (entry != null) {
                            if (entry.lockedByAny()) {
                                info("Near entry is still locked [i=" + i + ", entry=" + entry + ']');

                                return false;
                            }
                        }

                        entry = cache.context().near().dht().peekEx(key);

                        if (entry != null) {
                            if (entry.lockedByAny()) {
                                info("DHT entry is still locked [i=" + i + ", entry=" + entry + ']');

                                return false;
                            }
                        }
                    }

                    return true;
                }
                catch (GridCacheEntryRemovedException ignore) {
                    info("Entry was removed, will retry");

                    return false;
                }
            }
        }, 10_000);
    }

    /**
     * @return Key.
     */
    protected int generateNearKey() {
        CacheAffinity<Object> aff = ignite(0).affinity(null);

        int key = 0;

        while (true) {
            boolean primary = aff.isPrimary(ignite(1).cluster().localNode(), key);
            boolean primaryOrBackup = aff.isPrimaryOrBackup(ignite(0).cluster().localNode(), key);

            if (primary && !primaryOrBackup)
                return key;

            key++;
        }
    }
}
