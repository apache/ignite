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
import java.util.ArrayList;
import javax.cache.configuration.Factory;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.util.TransientSerializable;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Transactions configuration.
 */
@TransientSerializable(methodName = "transientSerializableFields")
public class TransactionConfiguration implements Serializable {
    /** */
    private static final IgniteProductVersion TX_PME_TIMEOUT_SINCE = IgniteProductVersion.fromString("2.5.1");

    /** */
    private static final IgniteProductVersion DEADLOCK_TIMEOUT_SINCE = IgniteProductVersion.fromString("2.7.3");

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

    /** Transaction timeout on partition map synchronization. */
    public static final long TX_TIMEOUT_ON_PARTITION_MAP_EXCHANGE = 0;

    /** Default timeout before starting deadlock detection. */
    public static final long DFLT_DEADLOCK_TIMEOUT = 10_000;

    /** 
      * Default size of pessimistic transactions log.
      * @deprecated Pessimistic tx log linger property has no effect.
      */
    public static final int DFLT_PESSIMISTIC_TX_LOG_LINGER = 10_000;

    /** Default transaction serializable flag. */
    private boolean txSerEnabled = DFLT_TX_SERIALIZABLE_ENABLED;

    /** Transaction isolation. */
    private TransactionIsolation dfltIsolation = DFLT_TX_ISOLATION;

    /** Cache concurrency. */
    private TransactionConcurrency dfltConcurrency = DFLT_TX_CONCURRENCY;

    /** Default transaction timeout. */
    private long dfltTxTimeout = DFLT_TRANSACTION_TIMEOUT;

    /**
     * Transaction timeout on partition map exchange.
     * Volatile in order to be changed dynamically.
     */
    private volatile long txTimeoutOnPartitionMapExchange = TX_TIMEOUT_ON_PARTITION_MAP_EXCHANGE;

    /** Timeout before starting deadlock detection. */
    private long deadlockTimeout = DFLT_DEADLOCK_TIMEOUT;

    /** Pessimistic tx log size. */
    @Deprecated
    private int pessimisticTxLogSize;

    /** Pessimistic tx log linger. */
    @Deprecated
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
        txTimeoutOnPartitionMapExchange = cfg.getTxTimeoutOnPartitionMapExchange();
        deadlockTimeout = cfg.getDeadlockTimeout();
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
     * @return {@code this} for chaining.
     */
    @Deprecated
    public TransactionConfiguration setTxSerializableEnabled(boolean txSerEnabled) {
        this.txSerEnabled = txSerEnabled;

        return this;
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
     * @return {@code this} for chaining.
     */
    public TransactionConfiguration setDefaultTxConcurrency(TransactionConcurrency dfltConcurrency) {
        this.dfltConcurrency = dfltConcurrency;

        return this;
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
     * @return {@code this} for chaining.
     */
    public TransactionConfiguration setDefaultTxIsolation(TransactionIsolation dfltIsolation) {
        this.dfltIsolation = dfltIsolation;

        return this;
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
     * @return {@code this} for chaining.
     */
    public TransactionConfiguration setDefaultTxTimeout(long dfltTxTimeout) {
        this.dfltTxTimeout = dfltTxTimeout;

        return this;
    }

    /**
     * Some Ignite operations provoke partition map exchange process within Ignite to ensure the partitions distribution
     * state is synchronized cluster-wide. Topology update events and a start of a new distributed cache are examples
     * of those operations.
     * <p>
     * When the partition map exchange starts, Ignite acquires a global lock at a particular stage. The lock can't be
     * obtained until pending transactions are running in parallel. If there is a transaction that runs for a while,
     * then it will prevent the partition map exchange process from the start freezing some operations such as a new
     * node join process.
     * <p>
     * This property allows to rollback such long transactions to let Ignite acquire the lock faster and initiate the
     * partition map exchange process. The timeout is enforced only at the time of the partition map exchange process.
     * <p>
     * If not set, default value is {@link #TX_TIMEOUT_ON_PARTITION_MAP_EXCHANGE} which means transactions will never be
     * rolled back on partition map exchange.
     *
     * @return Transaction timeout for partition map synchronization in milliseconds.
     */
    public long getTxTimeoutOnPartitionMapExchange() {
        return txTimeoutOnPartitionMapExchange;
    }

    /**
     * Sets the transaction timeout that will be enforced if the partition map exchange process starts.
     *
     * @param txTimeoutOnPartitionMapExchange Transaction timeout value in milliseconds.
     * @return {@code this} for chaining.
     */
    public TransactionConfiguration setTxTimeoutOnPartitionMapExchange(long txTimeoutOnPartitionMapExchange) {
        this.txTimeoutOnPartitionMapExchange = txTimeoutOnPartitionMapExchange;

        return this;
    }

    /**
     * <b>This is an experimental feature. Transactional SQL is currently in a beta status.</b>
     * <p>
     * Transaction deadlocks occurred for caches configured with {@link CacheAtomicityMode#TRANSACTIONAL_SNAPSHOT}
     * can be resolved automatically.
     * <p>
     * Deadlock detection starts when one transaction is waiting for an entry lock more than a timeout specified by
     * this property.
     * <p>
     * Timeout is specified in milliseconds and {@code 0} means that automatic deadlock detection is disabled. Default
     * value is defined by {@link #DFLT_DEADLOCK_TIMEOUT}.
     *
     * @return Timeout before starting deadlock detection.
     */
    @IgniteExperimental
    public long getDeadlockTimeout() {
        return deadlockTimeout;
    }

    /**
     * <b>This is an experimental feature. Transactional SQL is currently in a beta status.</b>
     * <p>
     * Sets a timeout before starting deadlock detection for caches configured with
     * {@link CacheAtomicityMode#TRANSACTIONAL_SNAPSHOT}.
     * <p>
     * Timeout is specified in milliseconds and {@code 0} means that automatic deadlock detection is disabled. Default
     * value is defined by {@link #DFLT_DEADLOCK_TIMEOUT}.
     *
     * @param deadlockTimeout Timeout value in milliseconds.
     * @return {@code this} for chaining.
     */
    @IgniteExperimental
    public TransactionConfiguration setDeadlockTimeout(long deadlockTimeout) {
        this.deadlockTimeout = deadlockTimeout;

        return this;
    }

    /**
     * Gets size of pessimistic transactions log stored on node in order to recover transaction commit if originating
     * node has left grid before it has sent all messages to transaction nodes.
     * <p>
     * If not set, default value is {@code 0} which means unlimited log size.
     *
     * @return Pessimistic transaction log size.
     * @deprecated Pessimistic tx log size property has no effect.
     */
    @Deprecated
    public int getPessimisticTxLogSize() {
        return pessimisticTxLogSize;
    }

    /**
     * Sets pessimistic transactions log size.
     *
     * @param pessimisticTxLogSize Pessimistic transactions log size.
     * @see #getPessimisticTxLogSize()
     * @return {@code this} for chaining.
     * @deprecated Pessimistic tx log size property has no effect.
     */
    @Deprecated
    public TransactionConfiguration setPessimisticTxLogSize(int pessimisticTxLogSize) {
        this.pessimisticTxLogSize = pessimisticTxLogSize;

        return this;
    }

    /**
     * Gets delay, in milliseconds, after which pessimistic recovery entries will be cleaned up for failed node.
     * <p>
     * If not set, default value is {@link #DFLT_PESSIMISTIC_TX_LOG_LINGER}.
     *
     * @return Pessimistic log cleanup delay in milliseconds.
     * @deprecated Pessimistic tx log linger property has no effect.
     */
    @Deprecated
    public int getPessimisticTxLogLinger() {
        return pessimisticTxLogLinger;
    }

    /**
     * Sets cleanup delay for pessimistic transaction recovery log for failed node, in milliseconds.
     *
     * @param pessimisticTxLogLinger Pessimistic log cleanup delay.
     * @see #getPessimisticTxLogLinger()
     * @return {@code this} for chaining.
     * @deprecated Pessimistic tx log linger property has no effect.
     */
    @Deprecated
    public TransactionConfiguration setPessimisticTxLogLinger(int pessimisticTxLogLinger) {
        this.pessimisticTxLogLinger = pessimisticTxLogLinger;

        return this;
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
     * @return {@code this} for chaining.
     */
    @Deprecated
    public TransactionConfiguration setTxManagerLookupClassName(String tmLookupClsName) {
        this.tmLookupClsName = tmLookupClsName;

        return this;
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
     * @return {@code this} for chaining.
     */
    public <T> TransactionConfiguration setTxManagerFactory(Factory<T> factory) {
        txManagerFactory = factory;

        return this;
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
     * @return {@code this} for chaining.
     */
    public TransactionConfiguration setUseJtaSynchronization(boolean useJtaSync) {
        this.useJtaSync = useJtaSync;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionConfiguration.class, this);
    }

    /**
     * Excludes incompatible fields from serialization/deserialization process.
     *
     * @param ver Sender/Receiver node version.
     * @return Array of excluded from serialization/deserialization fields.
     */
    @SuppressWarnings("unused")
    private static String[] transientSerializableFields(IgniteProductVersion ver) {
        ArrayList<String> transients = new ArrayList<>(2);

        if (TX_PME_TIMEOUT_SINCE.compareToIgnoreTimestamp(ver) >= 0)
            transients.add("txTimeoutOnPartitionMapExchange");

        if (DEADLOCK_TIMEOUT_SINCE.compareToIgnoreTimestamp(ver) >= 0)
            transients.add("deadlockTimeout");

        return transients.isEmpty() ? null : transients.toArray(new String[transients.size()]);
    }
}
