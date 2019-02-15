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

package org.apache.ignite.cache.store;

import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * This class tests that redundant calls of {@link CacheStoreSessionListener#onSessionStart(CacheStoreSession)}
 * and {@link CacheStoreSessionListener#onSessionEnd(CacheStoreSession, boolean)} are not executed.
 */
@RunWith(JUnit4.class)
public class CacheStoreListenerRWThroughDisabledTransactionalCacheTest extends CacheStoreSessionListenerReadWriteThroughDisabledAbstractTest {
    /** */
    @Before
    public void beforeCacheStoreListenerRWThroughDisabledTransactionalCacheTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        return super.cacheConfiguration(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * Tests {@link IgniteCache#get(Object)} with disabled read-through and write-through modes.
     */
    @Test
    public void testTransactionalLookup() {
        testTransactionalLookup(OPTIMISTIC, READ_COMMITTED);
        testTransactionalLookup(OPTIMISTIC, REPEATABLE_READ);
        testTransactionalLookup(OPTIMISTIC, SERIALIZABLE);

        testTransactionalLookup(PESSIMISTIC, READ_COMMITTED);
        testTransactionalLookup(PESSIMISTIC, REPEATABLE_READ);
        testTransactionalLookup(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Transaction concurrency level.
     * @param isolation Transaction isolation level.
     */
    private void testTransactionalLookup(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        IgniteCache cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        Random r = new Random();

        try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
            for (int i = 0; i < CNT; ++i)
                cache.get(r.nextInt());

            tx.commit();
        }
    }

    /**
     * Tests {@link IgniteCache#put(Object, Object)} with disabled read-through and write-through modes.
     */
    @Test
    public void testTransactionalUpdate() {
        testTransactionalUpdate(OPTIMISTIC, READ_COMMITTED);
        testTransactionalUpdate(OPTIMISTIC, REPEATABLE_READ);
        testTransactionalUpdate(OPTIMISTIC, SERIALIZABLE);

        testTransactionalUpdate(PESSIMISTIC, READ_COMMITTED);
        testTransactionalUpdate(PESSIMISTIC, REPEATABLE_READ);
        testTransactionalUpdate(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Transaction concurrency level.
     * @param isolation Transaction isolation level.
     */
    private void testTransactionalUpdate(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        IgniteCache cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        Random r = new Random();

        try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
            for (int i = 0; i < CNT; ++i)
                cache.put(r.nextInt(), "test-value");

            tx.commit();
        }
    }

    /**
     * Tests {@link IgniteCache#remove(Object)} with disabled read-through and write-through modes.
     */
    @Test
    public void testTransactionalRemove() {
        testTransactionalRemove(OPTIMISTIC, READ_COMMITTED);
        testTransactionalRemove(OPTIMISTIC, REPEATABLE_READ);
        testTransactionalRemove(OPTIMISTIC, SERIALIZABLE);

        testTransactionalRemove(PESSIMISTIC, READ_COMMITTED);
        testTransactionalRemove(PESSIMISTIC, REPEATABLE_READ);
        testTransactionalRemove(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Transaction concurrency level.
     * @param isolation Transaction isolation level.
     */
    private void testTransactionalRemove(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        IgniteCache cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        Random r = new Random();

        try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
            for (int i = 0; i < CNT; ++i) {
                int key = r.nextInt();

                cache.put(key, "test-value");

                cache.remove(key, "test-value");
            }

            tx.commit();
        }
    }
}
