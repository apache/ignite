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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TxSavepointsSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * Tests savepoint.
     */
    public void testSavepoints() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<String, Integer>>() {
            @Override public void applyx(Ignite ignite, IgniteCache<String, Integer> cache) throws Exception {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values())
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        cache.put("key", 0);

                        try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                            cache.put("key", 1);

                            tx.savepoint("s1");

                            cache.put("key", 2);

                            tx.savepoint("s2");

                            cache.put("key", 3);

                            tx.savepoint("s3");

                            tx.rollbackToSavepoint("s2");

                            assertEquals("Failed in " + concurrency + ' ' + isolation + " transaction.",
                                (Integer)2, cache.get("key"));

                            tx.rollbackToSavepoint("s1");

                            assertEquals("Failed in " + concurrency + ' ' + isolation + " transaction.",
                                (Integer)1, cache.get("key"));

                            tx.commit();
                        }

                        assertEquals("Failed in " + concurrency + ' ' + isolation + " transaction.",
                            (Integer)1, cache.get("key"));
                    }
            }});
    }

    /**
     * Tests valid and invalid rollbacks to savepoint.
     */
    public void testFailRollbackToSavepoint() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<String, Integer>>() {
            @Override public void applyx(Ignite ignite, IgniteCache<String, Integer> cache) throws Exception {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values())
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        GridTestUtils.assertThrows(
                            log, () -> {
                                cache.put("key", 0);

                                try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                                    cache.put("key", 1);

                                    tx.savepoint("s1");

                                    cache.put("key", 2);

                                    tx.savepoint("s2");

                                    cache.put("key", 3);

                                    tx.savepoint("s3");

                                    tx.rollbackToSavepoint("s2");

                                    assertEquals("Failed in " + concurrency + ' ' + isolation + " transaction.",
                                        (Integer)2, cache.get("key"));

                                    tx.rollbackToSavepoint("s3");
                                }

                                return null;
                            },
                            IllegalArgumentException.class,
                            "No such savepoint.");
                    }
            }});
    }

    /**
     * Tests savepoint deleting.
     */
    public void testOverwriteSavepoints() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<String, Integer>>() {
            @Override public void applyx(Ignite ignite, IgniteCache<String, Integer> cache) throws Exception {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values())
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        GridTestUtils.assertThrows(
                            log, () -> {
                                cache.put("key", 0);

                                try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                                    cache.put("key", 1);

                                    tx.savepoint("s1");

                                    cache.put("key", 2);

                                    tx.savepoint("s2");

                                    cache.put("key", 3);

                                    tx.savepoint("s3");

                                    tx.savepoint("s1", true);

                                    tx.rollbackToSavepoint("s2");
                                }

                                return null;
                            },
                            IllegalArgumentException.class,
                            "No such savepoint.");
                    }
            }});
    }

    /**
     * Tests savepoint deleting.
     */
    public void testFailOverwriteSavepoints() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<String, Integer>>() {
            @Override public void applyx(Ignite ignite, IgniteCache<String, Integer> cache) throws Exception {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values())
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        GridTestUtils.assertThrows(log, () -> {
                            cache.put("key", 0);

                            try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                                cache.put("key", 1);

                                tx.savepoint("s1");

                                tx.savepoint("s1");
                            }

                            return null;
                        },
                        IllegalArgumentException.class,
                        "Savepoint \"s1\" already exists.");
                    }
            }});
    }

    /**
     * Tests rollbacks to the same savepoint instance.
     */
    public void testMultipleRollbackToSavepoint() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<String, Integer>>() {
            @Override public void applyx(Ignite ignite, IgniteCache<String, Integer> cache) throws Exception {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values())
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        cache.put("key", 0);

                        try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                            cache.put("key", 1);

                            tx.savepoint("s1");

                            cache.put("key", 2);

                            tx.savepoint("s2");

                            cache.put("key", 3);

                            tx.savepoint("s3");

                            tx.rollbackToSavepoint("s2");

                            assertEquals("Failed in " + concurrency + ' ' + isolation + " transaction.",
                                (Integer)2, cache.get("key"));

                            cache.put("key", 4);

                            assertEquals("Failed in " + concurrency + ' ' + isolation + " transaction.",
                                (Integer)4, cache.get("key"));

                            tx.rollbackToSavepoint("s2");

                            assertEquals("Failed in " + concurrency + ' ' + isolation + " transaction.",
                                (Integer)2, cache.get("key"));

                            tx.commit();
                        }

                        assertEquals("Failed in " + concurrency + ' ' + isolation + " transaction.",
                            (Integer)2, cache.get("key"));
                    }
            }});
    }

    /**
     * Tests savepoints in failed transaction.
     */
    public void testTransactionRollback() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<String, Integer>>() {
            @Override public void applyx(Ignite ignite, IgniteCache<String, Integer> cache) throws Exception {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values())
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                            cache.put("key", 1);

                            tx.savepoint("s1");

                            cache.put("key", 2);

                            tx.savepoint("s2");

                            cache.put("key", 3);

                            tx.savepoint("s3");

                            tx.releaseSavepoint("s3");

                            tx.rollbackToSavepoint("s2");

                            assertEquals("Failed in " + concurrency + ' ' + isolation + " transaction.",
                                (Integer)2, cache.get("key"));

                            tx.rollback();
                        }

                        assertEquals("Failed in " + concurrency + ' ' + isolation + " transaction.",
                            null, cache.get("key"));
                    }
            }});
    }

    /**
     * Tests two caches within one transaction.
     */
    public void testCrossCacheRollbackToSavepoint() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<String, Integer>>() {
            @Override public void applyx(Ignite ignite, IgniteCache<String, Integer> cache) throws Exception {
                IgniteCache<String, Integer> cache2 = grid(0)
                    .getOrCreateCache(new CacheConfiguration<String, Integer>(
                        cache.getConfiguration(CacheConfiguration.class)).setName("Second Cache"));

                for (TransactionConcurrency concurrency : TransactionConcurrency.values())
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        cache.put("key", 0);

                        cache2.put("key", 0);

                        try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                            cache.put("key", 1);

                            cache2.put("key", 1);

                            tx.savepoint("s1");

                            cache.put("key", 2);

                            tx.savepoint("s2");

                            cache.put("key", 3);

                            tx.savepoint("s3");

                            cache2.put("key", 2);

                            tx.rollbackToSavepoint("s2");

                            tx.commit();
                        }

                        assertEquals("Failed in " + concurrency + ' ' + isolation + " transaction.",
                            (Integer)2, cache.get("key"));

                        assertEquals("Failed in " + concurrency + ' ' + isolation + " transaction.",
                            (Integer)1, cache2.get("key"));
                    }
            }});
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullNameSavepoint() throws Exception {
        GridTestUtils.assertThrows(log, () -> {
                try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    tx.savepoint(null);
                }

                return null;
            },
            IllegalArgumentException.class,
            "Savepoint name can't be null."
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullNameRollbackToSavepoint() throws Exception {
        GridTestUtils.assertThrows(log, () -> {
                try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    tx.rollbackToSavepoint(null);
                }

                return null;
            },
            IllegalArgumentException.class,
            "Savepoint name can't be null."
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullNameReleaseSavepoint() throws Exception {
        GridTestUtils.assertThrows(log, () -> {
                try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    tx.releaseSavepoint(null);
                }

                return null;
            },
            IllegalArgumentException.class,
            "Savepoint name can't be null."
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetTimeoutAfterSavepoint() throws Exception {
        GridTestUtils.assertThrows(log, () -> {
                try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    tx.savepoint("name");

                    tx.timeout(1_000);
                }

                return null;
            },
            IllegalStateException.class,
            "Cannot change timeout after transaction has started:"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllOutTx() throws Exception {
        Set<String> keys = new HashSet<>();

        for (int i = 0; i < 10; i++)
            keys.add(String.valueOf(i));

        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<String, Integer>>() {
            @Override public void applyx(Ignite ignite, IgniteCache<String, Integer> cache) throws Exception {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values())
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            tx.savepoint("sp");

                            assertEquals("Broken savepoint in " + concurrency + ' ' + isolation +
                                " transaction.", 0, cache.getAllOutTx(keys).size());

                            IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(() -> {
                                boolean b = true;

                                    for (String e : keys)
                                        b &= cache.putIfAbsent(e, 1);

                                    return b;
                                },cacheMode().name() + "_get");

                            assertTrue(fut.get(1_000));

                            tx.rollbackToSavepoint("sp");

                            assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                                ' ' + isolation + " transaction.",
                                (Integer)keys.size(),
                                (Integer)cache.getAll(keys).values().stream().mapToInt(Integer::intValue).sum());

                            tx.commit();
                        }
                        finally {
                            cache.removeAll();
                        }
                    }
        }});
    }

    /**
     * @return Cache configurations to test.
     */
    private List<CacheConfiguration<String, Integer>> cacheConfigurations() {
        List<CacheConfiguration<String, Integer>> cfgs = new ArrayList<>();

        cfgs.add(cacheConfiguration(PARTITIONED, 0, false));
        cfgs.add(cacheConfiguration(PARTITIONED, 0, true));
        cfgs.add(cacheConfiguration(PARTITIONED, 1, false));
        cfgs.add(cacheConfiguration(PARTITIONED, 1, true));
        cfgs.add(cacheConfiguration(REPLICATED, 0, false));
        cfgs.add(cacheConfiguration(REPLICATED, 0, true));
        cfgs.add(cacheConfiguration(LOCAL, 0, false));
        cfgs.add(cacheConfiguration(LOCAL, 0, true));

        return cfgs;
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param nearCache If {@code true} near cache is enabled.
     * @return Cache configuration.
     */
    private CacheConfiguration<String, Integer> cacheConfiguration(CacheMode cacheMode, int backups,
        boolean nearCache) {
        CacheConfiguration<String, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());
        else
            ccfg.setNearConfiguration(null);

        return ccfg;
    }

    /**
     * @param c Closure.
     * @throws Exception If failed.
     */
    private void executeTestForAllCaches(CI2<Ignite, IgniteCache<String, Integer>> c) throws Exception {
        for (CacheConfiguration<String, Integer> ccfg : cacheConfigurations()) {
            log.info("Run test for cache [cache=" + ccfg.getCacheMode() +
                ", backups=" + ccfg.getBackups() +
                ", near=" + (ccfg.getNearConfiguration() != null) + "]");

            Ignite ignite = ignite(0);

            c.apply(ignite, ignite.cache(ccfg.getName()));
        }
    }
}
