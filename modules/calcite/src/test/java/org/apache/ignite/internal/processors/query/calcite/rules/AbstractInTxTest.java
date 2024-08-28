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

package org.apache.ignite.internal.processors.query.calcite.rules;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.processors.tx.AbstractTransactionalSqlTest;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.transactions.Transaction;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** */
@RunWith(Parameterized.class)
public abstract class AbstractInTxTest extends AbstractTransactionalSqlTest {
    /** */
    public enum TxDml {
        /** All put, remove and SQL dml will be executed inside transaction. */
        ALL,

        /** Only some DML operations will be executed inside transaction. */
        RANDOM,

        /** Don't use transaction for DML. */
        NONE
    }

    /** */
    @Parameterized.Parameter()
    public TxDml txDml;

    /** */
    protected TxDml currentMode;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "txDml={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(TxDml.values());
    }

    /** */
    protected Transaction tx;

    /** */
    protected <K, V> void put(Ignite node, IgniteCache<K, V> cache, K key, V val) {
        RunnableX action = () -> cache.put(key, val);

        RunnableX txAction = () -> {
            if (tx == null)
                tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED, getTestTimeout(), 100);
            else
                tx.resume();

            try {
                action.run();
            }
            finally {
                tx.suspend();
            }

        };

        switch (txDml) {
            case ALL:
                txAction.run();
                break;
            case NONE:
                action.run();
                break;
            case RANDOM:
                if (ThreadLocalRandom.current().nextBoolean())
                    action.run();
                else
                    txAction.run();
        }
    }
}
