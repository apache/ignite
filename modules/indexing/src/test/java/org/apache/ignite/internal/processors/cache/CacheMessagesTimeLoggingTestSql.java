/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.msgtimelogging.GridCacheMessagesTimeLoggingAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_MESSAGES_TIME_LOGGING;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Tests for network time logging metrics with sql load.
 */
public class CacheMessagesTimeLoggingTestSql extends GridCacheMessagesTimeLoggingAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IGNITE_ENABLE_MESSAGES_TIME_LOGGING, "true");

        startGrids(GRID_CNT);

        super.beforeTest();
    }

    /** */
    public void testAtomicCacheTimeLogging() throws Exception {
        IgniteCache<Integer, Integer> cache0 = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("atomic_cache")
                                                                            .setBackups(1)
                                                                            .setIndexedTypes(Integer.class, Integer.class)
                                                                            .setAtomicityMode(ATOMIC)
                                                                            .setWriteSynchronizationMode(PRIMARY_SYNC));

        populateCache(cache0);

        checkTimeLoggableMsgsConsistancy();

        additionalChecks();
    }

    /** */
    public void testTransactionalCacheTimeLogging() throws Exception {
        IgniteCache<Integer, Integer> cache0 = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
                                                                            .setName("transactional_cache")
                                                                            .setIndexedTypes(Integer.class, Integer.class)
                                                                            .setAtomicityMode(TRANSACTIONAL));

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            populateTransactionalCache(cache0);

            tx.commit();
        }

        checkTimeLoggableMsgsConsistancy();

        additionalChecks();
    }

    /** */
    public static void populateCache(IgniteCache<Integer, Integer> cache) {
        GridCacheMessagesTimeLoggingAbstractTest.populateCache(cache);

        for (int i = 100; i < 120; i++)
            cache.query(new SqlFieldsQuery("insert into Integer (_key, _val) values (?,?)").setArgs(i, i));

        for (int i = 100; i < 110; i++)
            cache.query(new SqlFieldsQuery("DELETE from Integer WHERE _key = " + i));

        for (int i = 110; i < 120; i++)
            cache.query(new SqlFieldsQuery("update Integer set _val = 0 where _key = ?").setArgs(i));
    }

    /**
     * Temporary workaround. use {@code populateCache} instead when
     * https://ggsystems.atlassian.net/browse/GG-24652 is fixed.
     */
    private static void populateTransactionalCache(IgniteCache<Integer, Integer> cache) {
        Map<Integer, Integer> entriesToAdd = new HashMap<>();
        Set<Integer> keysToRemove = new HashSet<>();
        Set<Integer> keysToGet = new HashSet<>();

        for (int i = 0; i < 20; i++) {
            cache.put(i, i);
            entriesToAdd.put(i + 20, i * 2);
        }

        cache.putAll(entriesToAdd);

        for (int i = 0; i < 10; i++) {
            cache.remove(i);
            keysToRemove.add(i + 20);
        }

        cache.removeAll(keysToRemove);

        for (int i = 0; i < 10; i++) {
            cache.get(i + 5);
            keysToGet.add(i);
        }

        cache.getAll(keysToGet);

        for (int i = 100; i < 120; i++)
            cache.query(new SqlFieldsQuery("insert into Integer (_key, _val) values (?,?)").setArgs(i, i));

        for (int i = 100; i < 110; i++)
            cache.query(new SqlFieldsQuery("DELETE from Integer WHERE _key = " + i));

        for (int i = 110; i < 120; i++)
            cache.query(new SqlFieldsQuery("update Integer set _val = 0 where _key = ?").setArgs(i));
    }

    /** */
    protected void additionalChecks() throws Exception {
        // No-oo
    }
}
