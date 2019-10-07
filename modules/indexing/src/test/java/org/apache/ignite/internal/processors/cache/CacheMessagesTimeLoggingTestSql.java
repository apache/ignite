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
            populateCache(cache0);

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

    /** */
    protected void additionalChecks() throws Exception {
        // No-oo
    }
}
