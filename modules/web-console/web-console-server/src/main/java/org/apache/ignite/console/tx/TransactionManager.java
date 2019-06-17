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

package org.apache.ignite.console.tx;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.console.db.NestedTransaction;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Transactions manager.
 */
@Service
public class TransactionManager {
    /** */
    private final Ignite ignite;

    /**
     * @param ignite Ignite.
     */
    @Autowired
    protected TransactionManager(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Starts new transaction with the specified concurrency and isolation.
     *
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @return New transaction.
     */
    public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        IgniteTransactions txs = ignite.transactions();

        Transaction curTx = txs.tx();

        if (curTx instanceof NestedTransaction)
            return curTx;

        return curTx == null ? txs.txStart(concurrency, isolation) : new NestedTransaction(curTx);
    }

    /**
     * Start transaction.
     *
     * @return Transaction.
     */
    public Transaction txStart() {
        return txStart(PESSIMISTIC, REPEATABLE_READ);
    }
}
