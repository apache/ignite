/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/**
 * Configuration validation for SQL configured caches.
 */
public class CacheMvccSqlConfigurationValidationTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupAtomicityModeMismatch1() throws Exception {
        Ignite node = startGrid();

        node.getOrCreateCache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQuery("CREATE TABLE City (id int primary key, name varchar, population int) WITH " +
                "\"atomicity=transactional_snapshot,cache_group=group1,template=partitioned,backups=3,cache_name=City\""))
            .getAll();;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                node.cache(DEFAULT_CACHE_NAME)
                    .query(new SqlFieldsQuery("CREATE TABLE Person (id int primary key, name varchar) WITH " +
                        "\"atomicity=transactional,cache_group=group1,template=partitioned,backups=3,cache_name=Person\""))
                    .getAll();

                return null;
            }
        }, CacheException.class, "Atomicity mode mismatch for caches related to the same group");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupAtomicityModeMismatch2() throws Exception {
        Ignite node = startGrid();

        node.getOrCreateCache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQuery("CREATE TABLE City (id int primary key, name varchar, population int) WITH " +
                "\"atomicity=transactional,cache_group=group1,template=partitioned,backups=3,cache_name=City\""));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                node.cache(DEFAULT_CACHE_NAME)
                    .query(new SqlFieldsQuery("CREATE TABLE Person (id int primary key, name varchar) WITH " +
                        "\"atomicity=transactional_snapshot,cache_group=group1,template=partitioned,backups=3,cache_name=Person\""))
                    .getAll();

                return null;
            }
        }, CacheException.class, "Atomicity mode mismatch for caches related to the same group");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxDifferentMvccSettingsTransactional() throws Exception {
        ccfg = defaultCacheConfiguration().setSqlSchema("PUBLIC");
        Ignite node = startGrid();

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        cache.query(new SqlFieldsQuery("CREATE TABLE Person (id int primary key, name varchar) WITH " +
                "\"atomicity=transactional_snapshot,template=partitioned,backups=1\"")).getAll();

        cache.query(new SqlFieldsQuery("CREATE TABLE City (id int primary key, name varchar, population int) WITH " +
            "\"atomicity=transactional,template=partitioned,backups=3\"")).getAll();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    cache.query(new SqlFieldsQuery("SELECT * FROM Person, City")).getAll();

                    tx.commit();
                }

                return null;
            }
        }, CacheException.class, "Caches with transactional_snapshot atomicity mode cannot participate in the same transaction");
    }
}
