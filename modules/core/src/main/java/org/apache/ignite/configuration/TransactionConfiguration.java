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
import javax.cache.configuration.Factory;
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

    /** {@code javax.transaction.TransactionManager} factory. */
    private Factory txManagerFactory;

    /**
     * Whether to use JTA {@code javax.transaction.Synchronization}
     * instead of {@code javax.transaction.xa.XAResource}.
     */
    private boolean useJtaSync;

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
        txManagerFactory = cfg.getTxManagerFactory();
        useJtaSync = cfg.isUseJtaSynchronization();
    }

    /**
     * Gets flag to enable/disable {@link TransactionIsolation#SERIALIZABLE} isolation
     * level for cache transactions. Serializable level does carry certain overhead and
     * if not used, should be disabled. Default value is {@code false}.
     *
     * @return {@code True} if serializable transactions are enabled, {@code false} otherwise.
     */
    @Deprecated
    public boolean isTxSerializableEnabled() {
        return txSerEnabled;
    }

    /**
     * @param txSerEnabled Flag to enable/disable serializable cache transactions.

     * @deprecated This method has no effect, {@link TransactionIsolation#SERIALIZABLE} isolation is always enabled.
     */
    @Deprecated
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
     * @deprecated Use {@link #getTxManagerFactory()} instead.
     */
    @Deprecated
    public String getTxManagerLookupClassName() {
        return tmLookupClsName;
    }

    /**
     * Sets look up mechanism for available {@code TransactionManager} implementation, if any.
     *
     * @param tmLookupClsName Name of class implementing GridCacheTmLookup interface that is used to
     *      receive JTA transaction manager.
     * @deprecated Use {@link #setTxManagerFactory(Factory)} instead.
     */
    @Deprecated
    public void setTxManagerLookupClassName(String tmLookupClsName) {
        this.tmLookupClsName = tmLookupClsName;
    }

    /**
     * Gets transaction manager factory for integration with JEE app servers.
     *
     * @param <T> Instance of {@code javax.transaction.TransactionManager}.
     * @return Transaction manager factory.
     * @see #isUseJtaSynchronization()
     */
    @SuppressWarnings("unchecked")
    public <T> Factory<T> getTxManagerFactory() {
        return txManagerFactory;
    }

    /**
     * Sets transaction manager factory for available {@code javax.transaction.TransactionManager} implementation,
     * if any.
     * <p>
     * It allows to use different transactional systems. Implement factory that produce native
     * {@code javax.transaction.TransactionManager} within your environment.
     * <p>
     * The following implementations are provided out of the box (jta module must be enabled):
     * <ul>
     * <li>
     *  {@code org.apache.ignite.cache.jta.jndi.CacheJndiTmFactory} utilizes configured JNDI names to look up
     *  a transaction manager.
     * </li>
     * <li>
     *  {@code org.apache.ignite.cache.jta.websphere.WebSphereTmFactory} an implementation of Transaction Manager
     *  factory to be used within WebSphere Application Server.
     * </li>
     * <li>
     *  {@code org.apache.ignite.cache.jta.websphere.WebSphereLibertyTmFactory} an implementation of Transaction Manager
     *  factory to be used within WebSphere Liberty.
     * </li>
     * </ul>
     *
     * Ignite will throw IgniteCheckedException if {@link Factory#create()} method throws any exception,
     * returns {@code null}-value or returns non-{@code TransactionManager} instance.
     *
     * @param factory Transaction manager factory.
     * @param <T> Instance of {@code javax.transaction.TransactionManager}.
     * @see #setUseJtaSynchronization(boolean)
     */
    public <T> void setTxManagerFactory(Factory<T> factory) {
        txManagerFactory = factory;
    }

    /**
     * @return Whether to use JTA {@code javax.transaction.Synchronization}
     *      instead of {@code javax.transaction.xa.XAResource}.
     * @see #getTxManagerFactory()
     */
    public boolean isUseJtaSynchronization() {
        return useJtaSync;
    }

    /**
     * Sets the flag that defines whether to use lightweight JTA synchronization callback to enlist
     * into JTA transaction instead of creating a separate XA resource. In some cases this can give
     * performance improvement, but keep in mind that most of the transaction managers do not allow
     * to add more that one callback to a single transaction.
     *
     * @param useJtaSync Whether to use JTA {@code javax.transaction.Synchronization}
     *      instead of {@code javax.transaction.xa.XAResource}.
     * @see #setTxManagerFactory(Factory)
     */
    public void setUseJtaSynchronization(boolean useJtaSync) {
        this.useJtaSync = useJtaSync;
    }
}
