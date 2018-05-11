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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Test ensures that the put operation does not hang during asynchronous cache destroy.
 */
public abstract class IgniteCacheTxPutOnDestroyTest extends IgniteCachePutOnDestroyTest {
    /** {@inheritDoc} */
    @Override public CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTxPutOnDestroyCacheOptimisticReadCommitted() throws IgniteCheckedException {
        doTestTxPutOnDestroyCache0(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTxPutOnDestroyCacheOptimisticRepeatableRead() throws IgniteCheckedException {
        doTestTxPutOnDestroyCache0(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTxPutOnDestroyCacheOptimisticSerializable() throws IgniteCheckedException {
        doTestTxPutOnDestroyCache0(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTxPutOnDestroyCachePessimisticReadCommitted() throws IgniteCheckedException {
        doTestTxPutOnDestroyCache0(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTxPutOnDestroyCachePessimisticRepeatableRead() throws IgniteCheckedException {
        doTestTxPutOnDestroyCache0(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTxPutOnDestroyCachePessimisticSerializable() throws IgniteCheckedException {
        doTestTxPutOnDestroyCache0(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Transaction concurrency level.
     * @param isolation Transaction isolation level.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestTxPutOnDestroyCache0(TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws IgniteCheckedException {
        for (int n = 0; n < ITER_CNT; n++)
            doTestPutOnCacheDestroy(concurrency, isolation);
    }
}
