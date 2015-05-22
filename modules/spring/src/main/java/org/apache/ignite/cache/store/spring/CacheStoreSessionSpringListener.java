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

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.*;
import org.springframework.jdbc.datasource.*;
import org.springframework.transaction.*;
import org.springframework.transaction.support.*;

import javax.cache.integration.*;
import javax.sql.*;

/**
 * Cache store session listener based on Spring cache manager.
 */
public class CacheStoreSessionSpringListener implements CacheStoreSessionListener, LifecycleAware {
    /** Session key for transaction status. */
    public static final String TX_STATUS_KEY = "__spring_tx_status_";

    /** Transaction manager. */
    private PlatformTransactionManager txMgr;

    /** Data source. */
    private DataSource dataSrc;

    /** Propagation behavior. */
    private int propagation = TransactionDefinition.PROPAGATION_REQUIRED;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /**
     * Sets transaction manager.
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

    /**
     * Sets propagation behavior.
     *
     * @param propagation Propagation behavior.
     */
    public void setPropagationBehavior(int propagation) {
        this.propagation = propagation;
    }

    /**
     * Gets propagation behavior.
     *
     * @return Propagation behavior.
     */
    public int getPropagationBehavior() {
        return propagation;
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
        if (ses.isWithinTransaction()) {
            try {
                TransactionDefinition def = definition(ses.transaction(), ses.cacheName());

                ses.properties().put(TX_STATUS_KEY, txMgr.getTransaction(def));
            }
            catch (TransactionException e) {
                throw new CacheWriterException("Failed to start store session [tx=" + ses.transaction() + ']', e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {
        if (ses.isWithinTransaction()) {
            TransactionStatus tx = ses.<String, TransactionStatus>properties().remove(TX_STATUS_KEY);

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
        def.setPropagationBehavior(propagation);

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
