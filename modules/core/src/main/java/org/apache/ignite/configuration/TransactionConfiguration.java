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
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Transactions configuration.
 */
public class TransactionConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default value for 'txSerializableEnabled' flag. */
    public static final boolean DFLT_TX_SERIALIZABLE_ENABLED = false;

    /** Default concurrency mode. */
    public static final TransactionConcurrency DFLT_TX_CONCURRENCY = TransactionConcurrency.PESSIMISTIC;

    /** Default transaction isolation level. */
    public static final TransactionIsolation DFLT_TX_ISOLATION = TransactionIsolation.REPEATABLE_READ;

    /** Default transaction timeout. */
    public static final long DFLT_TRANSACTION_TIMEOUT = 0;

    /** Default size of pessimistic transactions log. */
    public static final int DFLT_PESSIMISTIC_TX_LOG_LINGER = 10_000;

    /** Default transaction serializable flag. */
    private boolean txSerEnabled = DFLT_TX_SERIALIZABLE_ENABLED;

    /** Transaction isolation. */
    private TransactionIsolation dfltIsolation = DFLT_TX_ISOLATION;

    /** Cache concurrency. */
    private TransactionConcurrency dfltConcurrency = DFLT_TX_CONCURRENCY;

    /** Default transaction timeout. */
    private long dfltTxTimeout = DFLT_TRANSACTION_TIMEOUT;

    /** Pessimistic tx log size. */
    private int pessimisticTxLogSize;

    /** Pessimistic tx log linger. */
    private int pessimisticTxLogLinger = DFLT_PESSIMISTIC_TX_LOG_LINGER;

    /** Name of class implementing GridCacheTmLookup. */
    private String tmLookupClsName;

    /**
     * Empty constructor.
     */
    public TransactionConfiguration() {
        // No-op.
    }

    /**
     * @param cfg Configuration to copy.
     */
    public TransactionConfiguration(TransactionConfiguration cfg) {
        dfltConcurrency = cfg.getDefaultTxConcurrency();
        dfltIsolation = cfg.getDefaultTxIsolation();
        dfltTxTimeout = cfg.getDefaultTxTimeout();
        pessimisticTxLogLinger = cfg.getPessimisticTxLogLinger();
        pessimisticTxLogSize = cfg.getPessimisticTxLogSize();
        txSerEnabled = cfg.isTxSerializableEnabled();
        tmLookupClsName = cfg.getTxManagerLookupClassName();
    }

    /**
     * Gets flag to enable/disable {@link TransactionIsolation#SERIALIZABLE} isolation
     * level for cache transactions. Serializable level does carry certain overhead and
     * if not used, should be disabled. Default value is {@code false}.
     *
     * @return {@code True} if serializable transactions are enabled, {@code false} otherwise.
     */
    public boolean isTxSerializableEnabled() {
        return txSerEnabled;
    }

    /**
     * Enables/disables serializable cache transactions. See {@link #isTxSerializableEnabled()} for more information.
     *
     * @param txSerEnabled Flag to enable/disable serializable cache transactions.
     */
    public void setTxSerializableEnabled(boolean txSerEnabled) {
        this.txSerEnabled = txSerEnabled;
    }

    /**
     * Default cache transaction concurrency to use when one is not explicitly
     * specified. Default value is defined by {@link #DFLT_TX_CONCURRENCY}.
     *
     * @return Default cache transaction concurrency.
     * @see Transaction
     */
    public TransactionConcurrency getDefaultTxConcurrency() {
        return dfltConcurrency;
    }

    /**
     * Sets default transaction concurrency.
     *
     * @param dfltConcurrency Default cache transaction concurrency.
     */
    public void setDefaultTxConcurrency(TransactionConcurrency dfltConcurrency) {
        this.dfltConcurrency = dfltConcurrency;
    }

    /**
     * Default cache transaction isolation to use when one is not explicitly
     * specified. Default value is defined by {@link #DFLT_TX_ISOLATION}.
     *
     * @return Default cache transaction isolation.
     * @see Transaction
     */
    public TransactionIsolation getDefaultTxIsolation() {
        return dfltIsolation;
    }

    /**
     * Sets default transaction isolation.
     *
     * @param dfltIsolation Default cache transaction isolation.
     */
    public void setDefaultTxIsolation(TransactionIsolation dfltIsolation) {
        this.dfltIsolation = dfltIsolation;
    }

    /**
     * Gets default transaction timeout. Default value is defined by {@link #DFLT_TRANSACTION_TIMEOUT}
     * which is {@code 0} and means that transactions will never time out.
     *
     * @return Default transaction timeout.
     */
    public long getDefaultTxTimeout() {
        return dfltTxTimeout;
    }

    /**
     * Sets default transaction timeout in milliseconds. By default this value is defined by {@link
     * #DFLT_TRANSACTION_TIMEOUT}.
     *
     * @param dfltTxTimeout Default transaction timeout.
     */
    public void setDefaultTxTimeout(long dfltTxTimeout) {
        this.dfltTxTimeout = dfltTxTimeout;
    }

    /**
     * Gets size of pessimistic transactions log stored on node in order to recover transaction commit if originating
     * node has left grid before it has sent all messages to transaction nodes.
     * <p>
     * If not set, default value is {@code 0} which means unlimited log size.
     *
     * @return Pessimistic transaction log size.
     */
    public int getPessimisticTxLogSize() {
        return pessimisticTxLogSize;
    }

    /**
     * Sets pessimistic transactions log size.
     *
     * @param pessimisticTxLogSize Pessimistic transactions log size.
     * @see #getPessimisticTxLogSize()
     */
    public void setPessimisticTxLogSize(int pessimisticTxLogSize) {
        this.pessimisticTxLogSize = pessimisticTxLogSize;
    }

    /**
     * Gets delay, in milliseconds, after which pessimistic recovery entries will be cleaned up for failed node.
     * <p>
     * If not set, default value is {@link #DFLT_PESSIMISTIC_TX_LOG_LINGER}.
     *
     * @return Pessimistic log cleanup delay in milliseconds.
     */
    public int getPessimisticTxLogLinger() {
        return pessimisticTxLogLinger;
    }

    /**
     * Sets cleanup delay for pessimistic transaction recovery log for failed node, in milliseconds.
     *
     * @param pessimisticTxLogLinger Pessimistic log cleanup delay.
     * @see #getPessimisticTxLogLinger()
     */
    public void setPessimisticTxLogLinger(int pessimisticTxLogLinger) {
        this.pessimisticTxLogLinger = pessimisticTxLogLinger;
    }

    /**
     * Gets class name of transaction manager finder for integration for JEE app servers.
     *
     * @return Transaction manager finder.
     */
    public String getTxManagerLookupClassName() {
        return tmLookupClsName;
    }

    /**
     * Sets look up mechanism for available {@code TransactionManager} implementation, if any.
     *
     * @param tmLookupClsName Name of class implementing GridCacheTmLookup interface that is used to
     *      receive JTA transaction manager.
     */
    public void setTxManagerLookupClassName(String tmLookupClsName) {
        this.tmLookupClsName = tmLookupClsName;
    }
}