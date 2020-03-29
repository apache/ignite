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

package org.apache.ignite.client;

import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientTransactionConfiguration;
import org.apache.ignite.internal.client.thin.ClientServerError;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Thin client transactions facade.
 * <p>
 * Transactions are bound to the thread started the transaction. After that, each cache operation within this thread
 * will belong to the corresponding transaction until the transaction is committed, rolled back or closed.
 * <p>
 * Transactions are {@link AutoCloseable}, so they will automatically rollback unless explicitly committed.
 * <p>
 * Default values for transaction isolation level, concurrency and timeout can be configured via
 * {@link ClientConfiguration#setTransactionConfiguration(ClientTransactionConfiguration)} property.
 *
 * @see ClientTransactionConfiguration
 */
public interface ClientTransactions {
    /**
     * Starts a new transaction with the default isolation level, concurrency and timeout.
     *
     * @return New transaction.
     * @throws ClientException If some unfinished transaction has already started by this thread.
     */
    public ClientTransaction txStart() throws ClientServerError, ClientException;

    /**
     * Starts a new transaction with the specified concurrency and isolation.
     *
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @return New transaction.
     * @throws ClientException If some unfinished transaction has already started by this thread.
     */
    public ClientTransaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation)
        throws ClientServerError, ClientException;

    /**
     * Starts a new transaction with the specified isolation, concurrency and timeout.
     *
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @return New transaction.
     * @throws ClientException If some unfinished transaction has already started by this thread.
     */
    public ClientTransaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation, long timeout)
        throws ClientServerError, ClientException;

    /**
     * Returns instance of {@code ClientTransactions} to mark each new transaction with a specified label.
     *
     * @param lb Label.
     * @return {@code This} for chaining.
     * @throws NullPointerException If label is null.
     */
    public ClientTransactions withLabel(String lb) throws ClientException;
}
