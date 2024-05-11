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

package org.apache.ignite.transactions.spring;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.transactions.proxy.TransactionProxy;
import org.apache.ignite.internal.transactions.proxy.TransactionProxyFactory;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronizationUtils;

/** Abstract implementation of Spring Transaction manager with omitted Ignite cluster access logic. */
public abstract class AbstractSpringTransactionManager extends AbstractPlatformTransactionManager
    implements ResourceTransactionManager, ApplicationListener<ContextRefreshedEvent> {
    /** Transaction factory.*/
    private TransactionProxyFactory txFactory;

    /** Ignite logger. */
    private IgniteLogger log;

    /** Transaction concurrency level. */
    private TransactionConcurrency txConcurrency;

    /** Default transaction isolation. */
    private TransactionIsolation dfltTxIsolation;

    /** Default transaction timeout. */
    private long dfltTxTimeout;

    /**
     * Gets transaction concurrency level.
     *
     * @return Transaction concurrency level.
     */
    public TransactionConcurrency getTransactionConcurrency() {
        return txConcurrency;
    }

    /**
     * Sets transaction concurrency level.
     *
     * @param txConcurrency transaction concurrency level.
     */
    public void setTransactionConcurrency(TransactionConcurrency txConcurrency) {
        this.txConcurrency = txConcurrency;
    }

    /** {@inheritDoc} */
    @Override public void onApplicationEvent(ContextRefreshedEvent evt) {
        if (txConcurrency == null)
            txConcurrency = defaultTransactionConcurrency();

        dfltTxIsolation = defaultTransactionIsolation();

        dfltTxTimeout = defaultTransactionTimeout();

        log = log();

        txFactory = createTransactionFactory();
    }

    /** {@inheritDoc} */
    @Override protected Object doGetTransaction() throws TransactionException {
        IgniteTransactionObject txObj = new IgniteTransactionObject();

        txObj.setTransactionHolder(
            (IgniteTransactionHolder)TransactionSynchronizationManager.getResource(txFactory), false);

        return txObj;
    }

    /** {@inheritDoc} */
    @Override protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        if (definition.getIsolationLevel() == TransactionDefinition.ISOLATION_READ_UNCOMMITTED)
            throw new InvalidIsolationLevelException("Ignite does not support READ_UNCOMMITTED isolation level.");

        IgniteTransactionObject txObj = (IgniteTransactionObject)transaction;
        TransactionProxy tx = null;

        try {
            if (txObj.getTransactionHolder() == null || txObj.getTransactionHolder().isSynchronizedWithTransaction()) {
                long timeout = dfltTxTimeout;

                if (definition.getTimeout() > 0)
                    timeout = TimeUnit.SECONDS.toMillis(definition.getTimeout());

                TransactionProxy newTx = txFactory.txStart(txConcurrency,
                    convertToIgniteIsolationLevel(definition.getIsolationLevel()), timeout);

                if (log.isDebugEnabled())
                    log.debug("Started Ignite transaction: " + newTx);

                txObj.setTransactionHolder(new IgniteTransactionHolder(newTx), true);
            }

            txObj.getTransactionHolder().setSynchronizedWithTransaction(true);
            txObj.getTransactionHolder().setTransactionActive(true);

            tx = txObj.getTransactionHolder().getTransaction();

            // Bind the session holder to the thread.
            if (txObj.isNewTransactionHolder())
                TransactionSynchronizationManager.bindResource(txFactory, txObj.getTransactionHolder());
        }
        catch (Exception ex) {
            if (tx != null)
                tx.close();

            throw new CannotCreateTransactionException("Could not create Ignite transaction", ex);
        }
    }

    /** {@inheritDoc} */
    @Override protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        IgniteTransactionObject txObj = (IgniteTransactionObject)status.getTransaction();
        TransactionProxy tx = txObj.getTransactionHolder().getTransaction();

        if (status.isDebug() && log.isDebugEnabled())
            log.debug("Committing Ignite transaction: " + tx);

        try {
            tx.commit();
        }
        catch (Exception e) {
            throw new TransactionSystemException("Could not commit Ignite transaction", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        IgniteTransactionObject txObj = (IgniteTransactionObject)status.getTransaction();
        TransactionProxy tx = txObj.getTransactionHolder().getTransaction();

        if (status.isDebug() && log.isDebugEnabled())
            log.debug("Rolling back Ignite transaction: " + tx);

        try {
            tx.rollback();
        }
        catch (Exception e) {
            throw new TransactionSystemException("Could not rollback Ignite transaction", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void doSetRollbackOnly(DefaultTransactionStatus status) throws TransactionException {
        IgniteTransactionObject txObj = (IgniteTransactionObject)status.getTransaction();
        TransactionProxy tx = txObj.getTransactionHolder().getTransaction();

        assert tx != null;

        if (status.isDebug() && log.isDebugEnabled())
            log.debug("Setting Ignite transaction rollback-only: " + tx);

        tx.setRollbackOnly();
    }

    /** {@inheritDoc} */
    @Override protected void doCleanupAfterCompletion(Object transaction) {
        IgniteTransactionObject txObj = (IgniteTransactionObject)transaction;

        // Remove the transaction holder from the thread, if exposed.
        if (txObj.isNewTransactionHolder()) {
            TransactionProxy tx = txObj.getTransactionHolder().getTransaction();
            TransactionSynchronizationManager.unbindResource(txFactory);

            if (log.isDebugEnabled())
                log.debug("Releasing Ignite transaction: " + tx);
        }

        txObj.getTransactionHolder().clear();
    }

    /** {@inheritDoc} */
    @Override protected boolean isExistingTransaction(Object transaction) throws TransactionException {
        IgniteTransactionObject txObj = (IgniteTransactionObject)transaction;

        return (txObj.getTransactionHolder() != null && txObj.getTransactionHolder().isTransactionActive());
    }

    /** {@inheritDoc} */
    @Override public Object getResourceFactory() {
        return txFactory;
    }

    /**
     * @param isolationLevel Spring isolation level.
     * @return Ignite isolation level.
     */
    private TransactionIsolation convertToIgniteIsolationLevel(int isolationLevel) {
        TransactionIsolation isolation = dfltTxIsolation;

        switch (isolationLevel) {
            case TransactionDefinition.ISOLATION_READ_COMMITTED:
                isolation = TransactionIsolation.READ_COMMITTED;

                break;

            case TransactionDefinition.ISOLATION_REPEATABLE_READ:
                isolation = TransactionIsolation.REPEATABLE_READ;

                break;

            case TransactionDefinition.ISOLATION_SERIALIZABLE:
                isolation = TransactionIsolation.SERIALIZABLE;
        }

        return isolation;
    }

    /** @return Default transaction isolation. */
    protected abstract TransactionIsolation defaultTransactionIsolation();

    /** @return Default transaction timeout. */
    protected abstract long defaultTransactionTimeout();

    /** @return Default transaction concurrency. */
    protected abstract TransactionConcurrency defaultTransactionConcurrency();

    /** Creates instance of {@link TransactionProxyFactory} that will be used to start new Ignite transactions. */
    protected abstract TransactionProxyFactory createTransactionFactory();

    /** @return Ignite logger.  */
    protected abstract IgniteLogger log();

    /**
     * An object representing a managed Ignite transaction.
     */
    protected static class IgniteTransactionObject implements SmartTransactionObject {
        /** */
        private IgniteTransactionHolder txHolder;

        /** */
        private boolean newTxHolder;

        /**
         * Sets the resource holder being used to hold Ignite resources in the
         * transaction.
         *
         * @param txHolder the transaction resource holder
         * @param newTxHolder         true if the holder was created for this transaction,
         *                          false if it already existed
         */
        private void setTransactionHolder(IgniteTransactionHolder txHolder, boolean newTxHolder) {
            this.txHolder = txHolder;
            this.newTxHolder = newTxHolder;
        }

        /**
         * Returns the resource holder being used to hold Ignite resources in the
         * transaction.
         *
         * @return the transaction resource holder
         */
        protected IgniteTransactionHolder getTransactionHolder() {
            return txHolder;
        }

        /**
         * Returns true if the transaction holder was created for the current
         * transaction and false if it existed prior to the transaction.
         *
         * @return true if the holder was created for this transaction, false if it
         * already existed
         */
        private boolean isNewTransactionHolder() {
            return newTxHolder;
        }

        /** {@inheritDoc} */
        @Override public boolean isRollbackOnly() {
            return txHolder.isRollbackOnly();
        }

        /** {@inheritDoc} */
        @Override public void flush() {
            TransactionSynchronizationUtils.triggerFlush();
        }
    }
}
