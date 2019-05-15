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
package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionUnsupportedConcurrencyException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class CacheMvccSqlTxModesTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed
     */
    @Test
    public void testSqlTransactionModesNoMvcc() throws Exception {
        IgniteEx node = startGrid(0);

        IgniteCache<Object, Object> nonMvccCache = node.createCache(new CacheConfiguration<>("no-mvcc-cache")
            .setAtomicityMode(TRANSACTIONAL).setIndexedTypes(Integer.class, Integer.class));

        nonMvccCache.put(1, 1);

        for (TransactionConcurrency conc : TransactionConcurrency.values()) {
            for (TransactionIsolation iso : TransactionIsolation.values()) {
                try (Transaction tx = node.transactions().txStart(conc, iso)) {
                    nonMvccCache.query(new SqlFieldsQuery("SELECT * FROM Integer")).getAll();

                    tx.commit();
                }
                catch (Throwable t) {
                    log.error("Transaction failed: concurrency=" + conc + ", isolation=" + iso, t);

                    throw t;
                }
            }
        }
    }

    /**
     * @throws Exception If failed
     */
    @Test
    public void testSqlTransactionModesMvcc() throws Exception {
        IgniteEx node = startGrid(0);

        IgniteCache<Object, Object> mvccCache = node.createCache(new CacheConfiguration<>("mvcc-cache")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT).setIndexedTypes(Integer.class, Integer.class));

        mvccCache.put(1,1);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Transaction tx = node.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
                    mvccCache.query(new SqlFieldsQuery("SELECT * FROM Integer")).getAll();

                    tx.commit();
                }

                return null;
            }
        }, TransactionUnsupportedConcurrencyException.class, "Only pessimistic transactions are supported when MVCC is enabled");

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Transaction tx = node.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                    mvccCache.query(new SqlFieldsQuery("SELECT * FROM Integer")).getAll();

                    tx.commit();
                }

                return null;
            }
        }, TransactionUnsupportedConcurrencyException.class, "Only pessimistic transactions are supported when MVCC is enabled");

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Transaction tx = node.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                    mvccCache.query(new SqlFieldsQuery("SELECT * FROM Integer")).getAll();

                    tx.commit();
                }

                return null;
            }
        }, TransactionUnsupportedConcurrencyException.class, "Only pessimistic transactions are supported when MVCC is enabled");

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            mvccCache.query(new SqlFieldsQuery("SELECT * FROM Integer")).getAll();

            tx.commit();
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            mvccCache.query(new SqlFieldsQuery("SELECT * FROM Integer")).getAll();

            tx.commit();
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {
            mvccCache.query(new SqlFieldsQuery("SELECT * FROM Integer")).getAll();

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed
     */
    @Test
    public void testConsequentMvccNonMvccOperations() throws Exception {
        IgniteEx node = startGrid(0);

        IgniteCache<Object, Object> mvccCache = node.createCache(new CacheConfiguration<>("mvcc-cache")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT).setIndexedTypes(Integer.class, Integer.class));

        IgniteCache<Object, Object> nonMvccCache = node.createCache(new CacheConfiguration<>("no-mvcc-cache")
            .setAtomicityMode(TRANSACTIONAL).setIndexedTypes(Integer.class, Integer.class));

        nonMvccCache.put(1, 1);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            mvccCache.query(new SqlFieldsQuery("INSERT INTO Integer (_key, _val) VALUES (3,3)")).getAll();

            tx.commit();
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            nonMvccCache.put(2, 2);

            tx.commit();
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            mvccCache.query(new SqlFieldsQuery("INSERT INTO Integer (_key, _val) VALUES (5,5)")).getAll();

            tx.commit();
        }

        nonMvccCache.put(6, 6);

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            mvccCache.query(new SqlFieldsQuery("INSERT INTO Integer (_key, _val) VALUES (7,7)")).getAll();

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed
     */
    @Test
    public void testConsequentMvccNonMvccMixedOperations() throws Exception {
        IgniteEx node = startGrid(0);

        IgniteCache<Object, Object> mvccCache = node.createCache(new CacheConfiguration<>("mvcc-cache")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT).setIndexedTypes(Integer.class, Integer.class));

        IgniteCache<Object, Object> nonMvccCache = node.createCache(new CacheConfiguration<>("no-mvcc-cache")
            .setAtomicityMode(TRANSACTIONAL).setIndexedTypes(Integer.class, Integer.class));

        nonMvccCache.query(new SqlFieldsQuery("INSERT INTO Integer (_key, _val) VALUES (1,1)")).getAll();

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            nonMvccCache.query(new SqlFieldsQuery("SELECT * FROM Integer")).getAll();

            mvccCache.put(2, 2);

            tx.commit();
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            nonMvccCache.query(new SqlFieldsQuery("SELECT * FROM Integer")).getAll();

            mvccCache.query(new SqlFieldsQuery("INSERT INTO Integer (_key, _val) VALUES (3,3)")).getAll();

            tx.commit();
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            mvccCache.put(4, 4);

            nonMvccCache.query(new SqlFieldsQuery("SELECT * FROM Integer")).getAll();

            tx.commit();
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            mvccCache.query(new SqlFieldsQuery("INSERT INTO Integer (_key, _val) VALUES (5,5)")).getAll();

            nonMvccCache.query(new SqlFieldsQuery("SELECT * FROM Integer")).getAll();

            tx.commit();
        }
    }
}
