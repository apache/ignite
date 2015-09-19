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

package org.apache.ignite.internal.processors.cache.integration;

import java.util.HashMap;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCacheTxStoreSessionTest extends IgniteCacheStoreSessionAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreSessionTx() throws Exception {
        testTxPut(jcache(0), null, null);

        testTxPut(ignite(0).cache(CACHE_NAME1), null, null);

        testTxRemove(null, null);

        testTxPutRemove(null, null);

        for (TransactionConcurrency concurrency : F.asList(PESSIMISTIC)) {
            for (TransactionIsolation isolation : F.asList(REPEATABLE_READ)) {
                testTxPut(jcache(0), concurrency, isolation);

                testTxRemove(concurrency, isolation);

                testTxPutRemove(concurrency, isolation);
            }
        }
    }

    /**
     * @param concurrency Concurrency mode.
     * @param isolation Isolation mode.
     * @throws Exception If failed.
     */
    private void testTxPutRemove(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        log.info("Test tx put/remove [concurrency=" + concurrency + ", isolation=" + isolation + ']');

        IgniteCache<Integer, Integer> cache = jcache(0);

        List<Integer> keys = testKeys(cache, 3);

        Integer key1 = keys.get(0);
        Integer key2 = keys.get(1);
        Integer key3 = keys.get(2);

        try (Transaction tx = startTx(concurrency, isolation)) {
            log.info("Do tx put1.");

            cache.put(key1, key1);

            log.info("Do tx put2.");

            cache.put(key2, key2);

            log.info("Do tx remove.");

            cache.remove(key3);

            expData.add(new ExpectedData(true, "writeAll", new HashMap<>(), null));
            expData.add(new ExpectedData(true, "delete", F.<Object, Object>asMap(0, "writeAll"), null));
            expData.add(new ExpectedData(true, "sessionEnd", F.<Object, Object>asMap(0, "writeAll", 1, "delete"), null));

            log.info("Do tx commit.");

            tx.commit();
        }

        assertEquals(0, expData.size());
    }

    /**
     * @param cache Cache.
     * @param concurrency Concurrency mode.
     * @param isolation Isolation mode.
     * @throws Exception If failed.
     */
    private void testTxPut(IgniteCache<Object, Object> cache,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {
        log.info("Test tx put [concurrency=" + concurrency + ", isolation=" + isolation + ']');

        List<Integer> keys = testKeys(cache, 3);

        Integer key1 = keys.get(0);

        try (Transaction tx = startTx(concurrency, isolation)) {
            log.info("Do tx get.");
            expData.add(new ExpectedData(false, "load", new HashMap(), cache.getName()));
            expData.add(new ExpectedData(true, "sessionEnd", F.<Object, Object>asMap(0, "load"), cache.getName()));

            cache.get(key1);

            expData.clear();

            log.info("Do tx put.");

            cache.put(key1, key1);

            expData.add(new ExpectedData(true, "write", new HashMap<>(), cache.getName()));
            expData.add(new ExpectedData(true, "sessionEnd", F.<Object, Object>asMap(0, "write"), cache.getName()));

            log.info("Do tx commit.");

            tx.commit();
        }

        assertEquals(0, expData.size());

        Integer key2 = keys.get(1);
        Integer key3 = keys.get(2);

        try (Transaction tx = startTx(concurrency, isolation)) {
            log.info("Do tx put1.");

            cache.put(key2, key2);

            log.info("Do tx put2.");

            cache.put(key3, key3);

            expData.add(new ExpectedData(true, "writeAll", new HashMap<>(), cache.getName()));
            expData.add(new ExpectedData(true, "sessionEnd", F.<Object, Object>asMap(0, "writeAll"), cache.getName()));

            log.info("Do tx commit.");

            tx.commit();
        }

        assertEquals(0, expData.size());
    }

    /**
     * @param concurrency Concurrency mode.
     * @param isolation Isolation mode.
     * @throws Exception If failed.
     */
    private void testTxRemove(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        log.info("Test tx remove [concurrency=" + concurrency + ", isolation=" + isolation + ']');

        IgniteCache<Integer, Integer> cache = jcache(0);

        List<Integer> keys = testKeys(cache, 3);

        Integer key1 = keys.get(0);

        try (Transaction tx = startTx(concurrency, isolation)) {
            log.info("Do tx get.");

            cache.get(key1);

            log.info("Do tx remove.");

            cache.remove(key1, key1);

            expData.add(new ExpectedData(true, "delete", new HashMap<>(), null));
            expData.add(new ExpectedData(true, "sessionEnd", F.<Object, Object>asMap(0, "delete"), null));

            log.info("Do tx commit.");

            tx.commit();
        }

        assertEquals(0, expData.size());

        Integer key2 = keys.get(1);
        Integer key3 = keys.get(2);

        try (Transaction tx = startTx(concurrency, isolation)) {
            log.info("Do tx remove1.");

            cache.remove(key2, key2);

            log.info("Do tx remove2.");

            cache.remove(key3, key3);

            expData.add(new ExpectedData(true, "deleteAll", new HashMap<>(), null));
            expData.add(new ExpectedData(true, "sessionEnd", F.<Object, Object>asMap(0, "deleteAll"), null));

            log.info("Do tx commit.");

            tx.commit();
        }

        assertEquals(0, expData.size());
    }

    /**
     * @param concurrency Concurrency mode.
     * @param isolation Isolation mode.
     * @return Transaction.
     */
    private Transaction startTx(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        IgniteTransactions txs = ignite(0).transactions();

        if (concurrency == null)
            return txs.txStart();

        return txs.txStart(concurrency, isolation);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSessionCrossCacheTx() throws Exception {
        IgniteCache<Object, Object> cache0 = ignite(0).cache(null);

        IgniteCache<Object, Object> cache1 = ignite(0).cache(CACHE_NAME1);

        Integer key1 = primaryKey(cache0);
        Integer key2 = primaryKeys(cache1, 1, key1 + 1).get(0);

        try (Transaction tx = startTx(null, null)) {
            cache0.put(key1, 1);

            cache1.put(key2, 0);

            expData.add(new ExpectedData(true, "write", new HashMap<>(), null));
            expData.add(new ExpectedData(true, "write", F.<Object, Object>asMap(0, "write"), CACHE_NAME1));
            expData.add(new ExpectedData(true, "sessionEnd", F.<Object, Object>asMap(0, "write", 1, "write"), null));

            tx.commit();
        }

        assertEquals(0, expData.size());

        try (Transaction tx = startTx(null, null)) {
            cache1.put(key1, 1);

            cache0.put(key2, 0);

            expData.add(new ExpectedData(true, "write", new HashMap<>(), CACHE_NAME1));
            expData.add(new ExpectedData(true, "write", F.<Object, Object>asMap(0, "write"), null));
            expData.add(new ExpectedData(true, "sessionEnd", F.<Object, Object>asMap(0, "write", 1, "write"), null));

            tx.commit();
        }

        assertEquals(0, expData.size());
    }
}