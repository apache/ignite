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

package org.apache.ignite.internal.processors.cache.consistency;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

/**
 *
 */
public class ExplicitTransactionalCacheConsistencyTest extends AbstractCacheConsistencyTest {
    /**
     *
     */
    @Test
    public void test() throws Exception {
        test(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        test(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        test(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

        test(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        test(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        test(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);
    }

    /**
     *
     */
    protected void test(TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {
        test(concurrency, isolation, true);
        test(concurrency, isolation, false);
    }

    /**
     *
     */
    private void test(TransactionConcurrency concurrency,
        TransactionIsolation isolation, boolean raw /*getEntry() or just get()*/) throws Exception {
        for (Ignite node : G.allGrids()) {
            testGet(node, concurrency, isolation, raw);
            testGetAllVariations(node, concurrency, isolation, raw);
        }
    }

    /** {@inheritDoc} */
    protected void testGet(Ignite initiator, TransactionConcurrency concurrency,
        TransactionIsolation isolation, boolean raw) throws Exception {
        prepareAndCheck(initiator, 1,
            (ValidatorData data) -> {
                try (Transaction tx = initiator.transactions().txStart(concurrency, isolation)) {
                    GET_CHECK_AND_FIX.accept(data); // Recovery (inside tx).
                    ENSURE_FIXED.accept(data); // Checks (inside tx).

                    try {
                        tx.commit();
                    }
                    catch (TransactionRollbackException e) {
                        fail("Should not happen. " + iterableKey);
                    }
                }

                ENSURE_FIXED.accept(data); // Checks (outside tx).
            }, raw);
    }

    /**
     *
     */
    private void testGetAllVariations(Ignite initiator, TransactionConcurrency concurrency,
        TransactionIsolation isolation, boolean raw) throws Exception {
        testGetAll(initiator, concurrency, isolation, 1, raw); // 1 (all keys available at primary)
        testGetAll(initiator, concurrency, isolation, 2, raw); // less than backups
        testGetAll(initiator, concurrency, isolation, 3, raw); // equals to backups
        testGetAll(initiator, concurrency, isolation, 4, raw); // equals to backups + primary
        testGetAll(initiator, concurrency, isolation, 10, raw); // more than backups
    }

    /** {@inheritDoc} */
    protected void testGetAll(Ignite initiator, TransactionConcurrency concurrency,
        TransactionIsolation isolation, Integer cnt, boolean raw) throws Exception {
        prepareAndCheck(initiator, cnt,
            (ValidatorData data) -> {
                try (Transaction tx = initiator.transactions().txStart(concurrency, isolation)) {
                    GETALL_CHECK_AND_FIX.accept(data); // Recovery (inside tx).
                    ENSURE_FIXED.accept(data); // Checks (inside tx).

                    try {
                        tx.commit();
                    }
                    catch (TransactionRollbackException e) {
                        fail("Should not happen. " + iterableKey);
                    }
                }

                ENSURE_FIXED.accept(data); // Checks (outside tx).
            }, raw);
    }
}
