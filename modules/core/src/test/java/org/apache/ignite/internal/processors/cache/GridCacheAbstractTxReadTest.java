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
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Tests value read inside transaction.
 */
public abstract class GridCacheAbstractTxReadTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheStoreFactory(null);

        return cfg;
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    public void testTxReadOptimisticReadCommitted() throws IgniteCheckedException {
        checkTransactionalRead(IgniteTxConcurrency.OPTIMISTIC, IgniteTxIsolation.READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    public void testTxReadOptimisticRepeatableRead() throws IgniteCheckedException {
        checkTransactionalRead(IgniteTxConcurrency.OPTIMISTIC, IgniteTxIsolation.REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    public void testTxReadOptimisticSerializable() throws IgniteCheckedException {
        checkTransactionalRead(IgniteTxConcurrency.OPTIMISTIC, IgniteTxIsolation.SERIALIZABLE);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    public void testTxReadPessimisticReadCommitted() throws IgniteCheckedException {
        checkTransactionalRead(IgniteTxConcurrency.PESSIMISTIC, IgniteTxIsolation.READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    public void testTxReadPessimisticRepeatableRead() throws IgniteCheckedException {
        checkTransactionalRead(IgniteTxConcurrency.PESSIMISTIC, IgniteTxIsolation.REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    public void testTxReadPessimisticSerializable() throws IgniteCheckedException {
        checkTransactionalRead(IgniteTxConcurrency.PESSIMISTIC, IgniteTxIsolation.SERIALIZABLE);
    }

    /**
     * Tests sequential value write and read inside transaction.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws IgniteCheckedException If failed
     */
    protected void checkTransactionalRead(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation)
        throws IgniteCheckedException {
        IgniteCache<String, Integer> cache = jcache(0);

        cache.clear();

        IgniteTx tx = grid(0).transactions().txStart(concurrency, isolation);

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
