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

package org.apache.ignite.configuration;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Thin client transactions configuration.
 */
public class ClientTransactionConfiguration implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Default concurrency mode. */
    public static final TransactionConcurrency DFLT_TX_CONCURRENCY = TransactionConcurrency.PESSIMISTIC;

    /** Default transaction isolation level. */
    public static final TransactionIsolation DFLT_TX_ISOLATION = TransactionIsolation.REPEATABLE_READ;

    /** Default transaction timeout. */
    public static final long DFLT_TRANSACTION_TIMEOUT = 0;

    /** Transaction isolation. */
    private TransactionIsolation dfltIsolation = DFLT_TX_ISOLATION;

    /** Cache concurrency. */
    private TransactionConcurrency dfltConcurrency = DFLT_TX_CONCURRENCY;

    /** Default transaction timeout. */
    private long dfltTxTimeout = DFLT_TRANSACTION_TIMEOUT;

    /**
     * Empty constructor.
     */
    public ClientTransactionConfiguration() {
        // No-op.
    }

    /**
     * @param cfg Configuration to copy.
     */
    public ClientTransactionConfiguration(ClientTransactionConfiguration cfg) {
        dfltConcurrency = cfg.getDefaultTxConcurrency();
        dfltIsolation = cfg.getDefaultTxIsolation();
        dfltTxTimeout = cfg.getDefaultTxTimeout();
    }

    /**
     * Default transaction concurrency to use when one is not explicitly specified.
     * Default value is defined by {@link #DFLT_TX_CONCURRENCY}.
     *
     * @return Default cache transaction concurrency.
     */
    public TransactionConcurrency getDefaultTxConcurrency() {
        return dfltConcurrency;
    }

    /**
     * Sets default transaction concurrency.
     *
     * @param dfltConcurrency Default transaction concurrency.
     * @return {@code this} for chaining.
     */
    public ClientTransactionConfiguration setDefaultTxConcurrency(TransactionConcurrency dfltConcurrency) {
        this.dfltConcurrency = dfltConcurrency;

        return this;
    }

    /**
     * Default transaction isolation to use when one is not explicitly specified.
     * Default value is defined by {@link #DFLT_TX_ISOLATION}.
     *
     * @return Default transaction isolation.
     * @see Transaction
     */
    public TransactionIsolation getDefaultTxIsolation() {
        return dfltIsolation;
    }

    /**
     * Sets default transaction isolation.
     *
     * @param dfltIsolation Default transaction isolation.
     * @return {@code this} for chaining.
     */
    public ClientTransactionConfiguration setDefaultTxIsolation(TransactionIsolation dfltIsolation) {
        this.dfltIsolation = dfltIsolation;

        return this;
    }

    /**
     * Gets default transaction timeout.
     * Default value is defined by {@link #DFLT_TRANSACTION_TIMEOUT} which is {@code 0} and means that transactions
     * will never time out.
     *
     * @return Default transaction timeout.
     */
    public long getDefaultTxTimeout() {
        return dfltTxTimeout;
    }

    /**
     * Sets default transaction timeout in milliseconds.
     *
     * @param dfltTxTimeout Default transaction timeout.
     * @return {@code this} for chaining.
     */
    public ClientTransactionConfiguration setDefaultTxTimeout(long dfltTxTimeout) {
        this.dfltTxTimeout = dfltTxTimeout;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientTransactionConfiguration.class, this);
    }
}
