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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests value read inside transaction.
 */
public abstract class GridCacheAbstractTxReadTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    @Test
    public void testTxReadOptimisticReadCommitted() throws IgniteCheckedException {
        checkTransactionalRead(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    @Test
    public void testTxReadOptimisticRepeatableRead() throws IgniteCheckedException {
        checkTransactionalRead(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    @Test
    public void testTxReadOptimisticSerializable() throws IgniteCheckedException {
        checkTransactionalRead(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    @Test
    public void testTxReadPessimisticReadCommitted() throws IgniteCheckedException {
        checkTransactionalRead(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    @Test
    public void testTxReadPessimisticRepeatableRead() throws IgniteCheckedException {
        checkTransactionalRead(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    @Test
    public void testTxReadPessimisticSerializable() throws IgniteCheckedException {
        checkTransactionalRead(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     * Tests sequential value write and read inside transaction.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws IgniteCheckedException If failed
     */
    protected void checkTransactionalRead(TransactionConcurrency concurrency, TransactionIsolation isolation)
        throws IgniteCheckedException {
        IgniteCache<String, Integer> cache = jcache(0);

        cache.clear();

        Transaction tx = grid(0).transactions().txStart(concurrency, isolation);

        try {
            cache.put("key", 1);

            assertEquals("Invalid value after put", 1, cache.get("key").intValue());

            tx.commit();
        }
        finally {
            tx.close();
        }

        assertEquals("Invalid cache size after put", 1, cache.size());

        try {
            tx = grid(0).transactions().txStart(concurrency, isolation);

            assertEquals("Invalid value inside transactional read", Integer.valueOf(1), cache.get("key"));

            tx.commit();
        }
        finally {
            tx.close();
        }
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }
}
