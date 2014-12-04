/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache;

/**
 * Transactions configuration.
 */
public class GridTransactionsConfiguration {
    /** Default value for 'txSerializableEnabled' flag. */
    public static final boolean DFLT_TX_SERIALIZABLE_ENABLED = false;

    /** Default concurrency mode. */
    public static final GridCacheTxConcurrency DFLT_TX_CONCURRENCY = GridCacheTxConcurrency.PESSIMISTIC;

    /** Default transaction isolation level. */
    public static final GridCacheTxIsolation DFLT_TX_ISOLATION = GridCacheTxIsolation.REPEATABLE_READ;

    /** Default transaction timeout. */
    public static final long DFLT_TRANSACTION_TIMEOUT = 0;

    /** Default size of pessimistic transactions log. */
    public static final int DFLT_PESSIMISTIC_TX_LOG_LINGER = 10_000;

    /** Default transaction serializable flag. */
    private boolean txSerEnabled = DFLT_TX_SERIALIZABLE_ENABLED;

    /** Transaction isolation. */
    private GridCacheTxIsolation dfltIsolation = DFLT_TX_ISOLATION;

    /** Cache concurrency. */
    private GridCacheTxConcurrency dfltConcurrency = DFLT_TX_CONCURRENCY;

    /** Default transaction timeout. */
    private long dfltTxTimeout = DFLT_TRANSACTION_TIMEOUT;

    /** Pessimistic tx log size. */
    private int pessimisticTxLogSize;

    /** Pessimistic tx log linger. */
    private int pessimisticTxLogLinger = DFLT_PESSIMISTIC_TX_LOG_LINGER;

    /**
     * Empty constructor.
     */
    public GridTransactionsConfiguration() {
        // No-op.
    }

    /**
     * @param cfg Configuration to copy.
     */
    public GridTransactionsConfiguration(GridTransactionsConfiguration cfg) {
        dfltConcurrency = cfg.getDefaultTxConcurrency();
        dfltIsolation = cfg.getDefaultTxIsolation();
        dfltTxTimeout = cfg.getDefaultTxTimeout();
        pessimisticTxLogLinger = cfg.getPessimisticTxLogLinger();
        pessimisticTxLogSize = cfg.getPessimisticTxLogSize();
        txSerEnabled = cfg.isTxSerializableEnabled();
    }

    /**
     * Gets flag to enable/disable {@link GridCacheTxIsolation#SERIALIZABLE} isolation
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
     * @see GridCacheTx
     */
    public GridCacheTxConcurrency getDefaultTxConcurrency() {
        return dfltConcurrency;
    }

    /**
     * Sets default transaction concurrency.
     *
     * @param dfltConcurrency Default cache transaction concurrency.
     */
    public void setDefaultTxConcurrency(GridCacheTxConcurrency dfltConcurrency) {
        this.dfltConcurrency = dfltConcurrency;
    }

    /**
     * Default cache transaction isolation to use when one is not explicitly
     * specified. Default value is defined by {@link #DFLT_TX_ISOLATION}.
     *
     * @return Default cache transaction isolation.
     * @see GridCacheTx
     */
    public GridCacheTxIsolation getDefaultTxIsolation() {
        return dfltIsolation;
    }

    /**
     * Sets default transaction isolation.
     *
     * @param dfltIsolation Default cache transaction isolation.
     */
    public void setDefaultTxIsolation(GridCacheTxIsolation dfltIsolation) {
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
}
