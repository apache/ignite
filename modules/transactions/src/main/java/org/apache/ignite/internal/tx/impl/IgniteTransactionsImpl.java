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

package org.apache.ignite.internal.tx.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;

/**
 * Transactions facade implementation.
 */
public class IgniteTransactionsImpl implements IgniteTransactions {
    private final TxManager txManager;

    /**
     * The constructor.
     *
     * @param txManager The manager.
     */
    public IgniteTransactionsImpl(TxManager txManager) {
        this.txManager = txManager;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTransactions withTimeout(long timeout) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Transaction begin() {
        return txManager.begin();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Transaction> beginAsync() {
        return CompletableFuture.completedFuture(txManager.begin());
    }

    /** {@inheritDoc} */
    @Override
    public void runInTransaction(Consumer<Transaction> clo) throws TransactionException {
        runInTransaction(tx -> {
            clo.accept(tx);
            return null;
        });
    }

    /** {@inheritDoc} */
    @Override
    public <T> T runInTransaction(Function<Transaction, T> clo) throws TransactionException {
        InternalTransaction tx = txManager.begin();

        Thread th = Thread.currentThread();

        txManager.setTx(tx);

        tx.thread(th);

        try {
            T ret = clo.apply(tx);

            tx.commit();

            return ret;
        } catch (Throwable t) {
            try {
                tx.rollback(); // Try rolling back on user exception.
            } catch (Exception e) {
                t.addSuppressed(e);
            }

            throw t;
        } finally {
            txManager.clearTx();
        }
    }
}
