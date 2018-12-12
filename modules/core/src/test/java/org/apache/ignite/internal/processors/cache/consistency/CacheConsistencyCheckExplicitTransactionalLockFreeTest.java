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
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;

/**
 *
 */
public class CacheConsistencyCheckExplicitTransactionalLockFreeTest
    extends CacheConsistencyCheckExplicitTransactionalAbstractTest {
    /** {@inheritDoc} */
    @Override public void test() throws Exception {
        test(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        test(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        test(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

        test(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
    }


    /** {@inheritDoc} */
    @Override protected void testGet(Ignite initiator, TransactionConcurrency concurrency,
        TransactionIsolation isolation, boolean raw) throws Exception {
        prepareAndCheck(initiator, 1,
            (T3<IgniteCache<Integer, Integer>, Map<Integer, T3<Map<Ignite, Integer>, Integer, Integer>>, Boolean> t) -> {
                try (Transaction tx = initiator.transactions().txStart(concurrency, isolation)) {
                    GET_CHECK_AND_FAIL.accept(t);

                    try {
                        tx.commit();

                        fail("Should not happen. " + iterableKey);
                    }
                    catch (TransactionRollbackException e) {
                        assertTrue(e.getCause() instanceof IgniteTxRollbackCheckedException);
                    }
                }
            }, raw);
    }


    /** {@inheritDoc} */
    @Override protected void testGetAll(Ignite initiator, TransactionConcurrency concurrency,
        TransactionIsolation isolation, Integer cnt, boolean raw) throws Exception {
        prepareAndCheck(initiator, cnt,
            (T3<IgniteCache<Integer, Integer>, Map<Integer, T3<Map<Ignite, Integer>, Integer, Integer>>, Boolean> t) -> {
                try (Transaction tx = initiator.transactions().txStart(concurrency, isolation)) {
                    GETALL_CHECK_AND_FAIL.accept(t);

                    try {
                        tx.commit();

                        fail("Should not happen. " + iterableKey);
                    }
                    catch (TransactionRollbackException e) {
                        assertTrue(e.getCause() instanceof IgniteTxRollbackCheckedException);
                    }
                }
            }, raw);
    }
}
