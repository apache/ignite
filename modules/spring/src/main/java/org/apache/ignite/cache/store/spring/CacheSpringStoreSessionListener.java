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

package org.apache.ignite.cache.store.spring;

import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

/**
 * Cache store session listener based on Spring transaction management.
 * <p>
 * This listener starts a new DB transaction for each session and commits
 * or rolls it back when session ends. If there is no ongoing
 * cache transaction, this listener is no-op.
 * <p>
 * Store implementation can use any Spring APIs like {@link JdbcTemplate}
 * and others. The listener will guarantee that if there is an
 * ongoing cache transaction, all store operations within this
 * transaction will be automatically enlisted in the same database
 * transaction.
 * <p>
 * {@link CacheSpringStoreSessionListener} requires that either
 * {@link #setTransactionManager(PlatformTransactionManager) transaction manager}
 * or {@link #setDataSource(DataSource) data source} is configured. If non of them is
 * provided, exception is thrown. Is both are provided, data source will be
 * ignored.
 * <p>
 * If there is a transaction, a {@link TransactionStatus} object will be saved
 * as a store session {@link CacheStoreSession#attachment() attachment}. It
 * can be used to acquire current DB transaction status.
 */
public class CacheSpringStoreSessionListener implements CacheStoreSessionListener, LifecycleAware {
    /** Transaction manager. */
    private PlatformTransactionManager txMgr;

    /** Data source. */
    private DataSource dataSrc;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /**
     * Sets transaction manager.
     * <p>
     * Either transaction manager or data source is required.
     * If none is provided, exception will be thrown on startup.
     *
     * @param txMgr Transaction manager.
     */
    public void setTransactionManager(PlatformTransactionManager txMgr) {
        this.txMgr = txMgr;
    }

    /**
     * Gets transaction manager.
     *
     * @return Transaction manager.
     */
    public PlatformTransactionManager getTransactionManager() {
        return txMgr;
    }

    /**
     * Sets data source.
     * <p>
     * Either transaction manager or data source is required.
     * If none is provided, exception will be thrown on startup.
     *
     * @param dataSrc Data source.
     */
    public void setDataSource(DataSource dataSrc) {
        this.dataSrc = dataSrc;
    }

    /**
     * Gets data source.
     *
     * @return Data source.
     */
    public DataSource getDataSource() {
        return dataSrc;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (txMgr == null && dataSrc == null)
            throw new IgniteException("Either transaction manager or data source is required by " +
                getClass().getSimpleName() + '.');

        if (dataSrc != null) {
            if (txMgr == null)
                txMgr = new DataSourceTransactionManager(dataSrc);
            else
                U.warn(log, "Data source configured in " + getClass().getSimpleName() +
                    " will be ignored (transaction manager is already set).");
        }

        assert txMgr != null;
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSessionStart(CacheStoreSession ses) {
        if (ses.isWithinTransaction() && ses.attachment() == null) {
            try {
                TransactionDefinition def = definition(ses.transaction(), ses.cacheName());

                ses.attach(txMgr.getTransaction(def));
            }
            catch (TransactionException e) {
                throw new CacheWriterException("Failed to start store session [tx=" + ses.transaction() + ']', e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {
        if (ses.isWithinTransaction()) {
            TransactionStatus tx = ses.attach(null);

            if (tx != null) {
                try {
                    if (commit)
                        txMgr.commit(tx);
                    else
                        txMgr.rollback(tx);
                }
                catch (TransactionException e) {
                    throw new CacheWriterException("Failed to end store session [tx=" + ses.transaction() + ']', e);
                }
            }
        }
    }

    /**
     * Gets DB transaction isolation level based on ongoing cache transaction isolation.
     *
     * @return DB transaction isolation.
     */
    private TransactionDefinition definition(Transaction tx, String cacheName) {
        assert tx != null;

        DefaultTransactionDefinition def = new DefaultTransactionDefinition();

        def.setName("Ignite Tx [cache=" + (cacheName != null ? cacheName : "<default>") + ", id=" + tx.xid() + ']');
        def.setIsolationLevel(isolationLevel(tx.isolation()));

        long timeoutSec = (tx.timeout() + 500) / 1000;

        if (timeoutSec > 0 && timeoutSec < Integer.MAX_VALUE)
            def.setTimeout((int)timeoutSec);

        return def;
    }

    /**
     * Gets DB transaction isolation level based on ongoing cache transaction isolation.
     *
     * @param isolation Cache transaction isolation.
     * @return DB transaction isolation.
     */
    private int isolationLevel(TransactionIsolation isolation) {
        switch (isolation) {
            case READ_COMMITTED:
                return TransactionDefinition.ISOLATION_READ_COMMITTED;

            case REPEATABLE_READ:
                return TransactionDefinition.ISOLATION_REPEATABLE_READ;

            case SERIALIZABLE:
                return TransactionDefinition.ISOLATION_SERIALIZABLE;

            default:
                throw new IllegalStateException(); // Will never happen.
        }
    }
}