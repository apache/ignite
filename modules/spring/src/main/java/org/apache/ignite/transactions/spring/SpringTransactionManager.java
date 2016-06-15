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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Implementation of Spring transaction abstraction based on Ignite transaction.
 * <h1 class="header">Overview</h1>
 * Spring transaction abstraction allows to enable declarative transaction management
 * and concentrate on business logic rather than transaction life-cycle.
 * For more information, refer to
 * <a href="http://docs.spring.io/spring/docs/current/spring-framework-reference/html/transaction.html">
 * Spring Transaction Abstraction documentation</a>.
 * <h1 class="header">How To Enable Transaction support</h1>
 * To enable declarative transaction management on Ignite cache in your Spring application,
 * you will need to do the following:
 * <ul>
 * <li>
 * Start an Ignite node with proper configuration in embedded mode
 * (i.e., in the same JVM where the application is running). It can
 * already have predefined caches, but it's not required - caches
 * will be created automatically on first access if needed.
 * </li>
 * <li>
 * Configure {@code SpringTransactionManager} as a transaction manager
 * in the Spring application context.
 * </li>
 * </ul>
 * {@code SpringTransactionManager} can start a node itself on its startup
 * based on provided Ignite configuration. You can provide path to a
 * Spring configuration XML file, like below (path can be absolute or
 * relative to {@code IGNITE_HOME}):
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:tx="http://www.springframework.org/schema/tx"
 *        xsi:schemaLocation="
 *            http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *            http://www.springframework.org/schema/tx http://www.springframework.org/schema/tx/spring-tx.xsd"&gt;
 *     &lt;-- Provide configuration file path. --&gt;
 *     &lt;bean id="transactionManager" class="org.apache.ignite.transactions.spring.SpringTransactionManager"&gt;
 *         &lt;property name="configurationPath" value="examples/config/spring-transaction.xml"/&gt;
 *     &lt;/bean&gt;
 *
 *     &lt;-- Use annotation-driven transaction configuration. --&gt;
 *     &lt;tx:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 * Or you can provide a {@link IgniteConfiguration} bean, like below:
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:tx="http://www.springframework.org/schema/tx"
 *        xsi:schemaLocation="
 *            http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *            http://www.springframework.org/schema/tx http://www.springframework.org/schema/tx/spring-tx.xsd"&gt;
 *     &lt;-- Provide configuration bean. --&gt;
 *     &lt;bean id="transactionManager" class="org.apache.ignite.transactions.spring.SpringTransactionManager"&gt;
 *         &lt;property name="configuration"&gt;
 *             &lt;bean id="gridCfg" class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
 *                 ...
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 *
 *     &lt;-- Use annotation-driven transaction configuration. --&gt;
 *     &lt;tx:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 * Note that providing both configuration path and configuration bean is illegal
 * and results in {@link IllegalArgumentException}.
 *
 * If you already have Ignite node running within your application,
 * simply provide correct Grid name, like below (if there is no Grid
 * instance with such name, exception will be thrown):
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:tx="http://www.springframework.org/schema/tx"
 *        xsi:schemaLocation="
 *            http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *            http://www.springframework.org/schema/tx http://www.springframework.org/schema/tx/spring-tx.xsd"&gt;
 *     &lt;-- Provide Grid name. --&gt;
 *     &lt;bean id="transactionManager" class="org.apache.ignite.transactions.spring.SpringTransactionManager"&gt;
 *         &lt;property name="instanceName" value="myGrid"/&gt;
 *     &lt;/bean>
 *
 *     &lt;-- Use annotation-driven transaction configuration. --&gt;
 *     &lt;tx:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 * This can be used, for example, when you are running your application
 * in a J2EE Web container and use {@ignitelink org.apache.ignite.startup.servlet.ServletContextListenerStartup}
 * for node startup.
 *
 * If neither {@link #setConfigurationPath(String) configurationPath},
 * {@link #setConfiguration(IgniteConfiguration) configuration}, nor
 * {@link #setInstanceName(String) instanceName} are provided, transaction manager
 * will try to use default Grid instance (the one with the {@code null}
 * name). If it doesn't exist, exception will be thrown.
 *
 * {@code SpringTransactionManager} can be configured to support Ignite transaction concurrency.
 * For this you need to provide {@code SpringTransactionManager} with transactionConcurrency property.
 * If this property is not set then default transaction concurrency will be used
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:tx="http://www.springframework.org/schema/tx"
 *        xsi:schemaLocation="
 *            http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *            http://www.springframework.org/schema/tx http://www.springframework.org/schema/tx/spring-tx.xsd"&gt;
 *     &lt;-- Provide Grid name. --&gt;
 *     &lt;bean id="transactionManager" class="org.apache.ignite.transactions.spring.SpringTransactionManager"&gt;
 *         &lt;property name="instanceName" value="myGrid"/&gt;
 *         &lt;property name="transactionConcurrency" value="OPTIMISTIC"/&gt;
 *     &lt;/bean>
 *
 *     &lt;-- Use annotation-driven transaction configuration. --&gt;
 *     &lt;tx:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 *
 * In case you need to support both "OPTIMISTIC" and "PESSIMISTIC" transaction concurrency in you application,
 * you need to create two transaction managers with different transaction concurrency
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:tx="http://www.springframework.org/schema/tx"
 *        xsi:schemaLocation="
 *            http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *            http://www.springframework.org/schema/tx http://www.springframework.org/schema/tx/spring-tx.xsd"&gt;
 *     &lt;bean id="optimisticTransactionManager" class="org.apache.ignite.transactions.spring.SpringTransactionManager"&gt;
 *         &lt;property name="instanceName" value="myGrid"/&gt;
 *         &lt;property name="transactionConcurrency" value="OPTIMISTIC"/&gt;
 *     &lt;/bean>
 *
 *     &lt;bean id="pessimisticTransactionManager" class="org.apache.ignite.transactions.spring.SpringTransactionManager"&gt;
 *         &lt;property name="instanceName" value="myGrid"/&gt;
 *         &lt;property name="transactionConcurrency" value="PESSIMISTIC"/&gt;
 *     &lt;/bean>
 *
 *     &lt;-- Use annotation-driven transaction configuration. --&gt;
 *     &lt;tx:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 * Then use them with qualifiers in your application:
 * <pre name="code" class="xml">
 * public class TransactionalService {
 *     {@literal @}Transactional("optimisticTransactionManager")
 *     public void doOptimistically() {
 *         ...
 *     }
 *
 *     {@literal @}Transactional("pessimisticTransactionManager")
 *     public void doPessimistically() {
 *         ...
 *     }
 * }
 * </pre>
 */
public class SpringTransactionManager extends AbstractPlatformTransactionManager
    implements ResourceTransactionManager, PlatformTransactionManager, InitializingBean {
    /**
     * Logger.
     */
    private IgniteLogger log;

    /**
     * Transaction concurrency level.
     */
    private TransactionConcurrency transactionConcurrency;

    /**
     * Grid configuration file path.
     */
    private String cfgPath;

    /**
     * Ignite configuration.
     */
    private IgniteConfiguration cfg;

    /**
     * Grid name.
     */
    private String instanceName;

    /**
     * Ignite instance.
     */
    private Ignite ignite;

    /**
     * Constructs the transaction manager with no target Ignite instance. An
     * instance must be set before use.
     */
    public SpringTransactionManager() {
        setNestedTransactionAllowed(false);
    }

    /**
     * Gets transaction concurrency level.
     *
     * @return Transaction concurrency level.
     */
    public TransactionConcurrency getTransactionConcurrency() {
        return transactionConcurrency;
    }

    /**
     * Sets transaction concurrency level.
     *
     * @param transactionConcurrency transaction concurrency level.
     */
    public void setTransactionConcurrency(TransactionConcurrency transactionConcurrency) {
        this.transactionConcurrency = transactionConcurrency;
    }

    /**
     * Gets configuration file path.
     *
     * @return Grid configuration file path.
     */
    public String getConfigurationPath() {
        return cfgPath;
    }

    /**
     * Sets configuration file path.
     *
     * @param cfgPath Grid configuration file path.
     */
    public void setConfigurationPath(String cfgPath) {
        this.cfgPath = cfgPath;
    }

    /**
     * Gets configuration bean.
     *
     * @return Grid configuration bean.
     */
    public IgniteConfiguration getConfiguration() {
        return cfg;
    }

    /**
     * Sets configuration bean.
     *
     * @param cfg Grid configuration bean.
     */
    public void setConfiguration(IgniteConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Gets grid name.
     *
     * @return Grid name.
     * @deprecated Use {@link #getInstanceName()} instead.
     */
    @Deprecated
    public String getGridName() {
        return instanceName;
    }

    /**
     * Sets grid name.
     *
     * @param instanceName Grid name.
     * @deprecated Use {@link #setInstanceName(String)} instead.
     */
    @Deprecated
    public void setGridName(String instanceName) {
        this.instanceName = instanceName;
    }

    /**
     * Gets grid name.
     * @return instanceName Grid instance name
     */
    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void afterPropertiesSet() throws Exception {
        assert ignite == null;

        if (cfgPath != null && cfg != null) {
            throw new IllegalArgumentException("Both 'configurationPath' and 'configuration' are " +
                    "provided. Set only one of these properties if you need to start a Ignite node inside of " +
                    "SpringCacheManager. If you already have a node running, omit both of them and set" +
                    "'instanceName' property.");
        }

        if (cfgPath != null)
            ignite = Ignition.start(cfgPath);
        else if (cfg != null)
            ignite = Ignition.start(cfg);
        else
            ignite = Ignition.ignite(instanceName);

        if (transactionConcurrency == null)
            transactionConcurrency = ignite.configuration().getTransactionConfiguration().getDefaultTxConcurrency();

        log = ignite.log();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected Object doGetTransaction() throws TransactionException {
        IgniteTransactionObject txObject = new IgniteTransactionObject();

        txObject.setTransactionHolder(
            (IgniteTransactionHolder)TransactionSynchronizationManager.getResource(this.ignite), false);

        return txObject;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        if (definition.getIsolationLevel() == TransactionDefinition.ISOLATION_READ_UNCOMMITTED)
            throw new InvalidIsolationLevelException("Ignite does not support READ_UNCOMMITTED isolation level.");

        IgniteTransactionObject txObject = (IgniteTransactionObject)transaction;
        Transaction tx = null;

        try {
            if (txObject.getTransactionHolder() == null || txObject.getTransactionHolder().isSynchronizedWithTransaction()) {
                long timeout = ignite.configuration().getTransactionConfiguration().getDefaultTxTimeout();

                if (definition.getTimeout() > 0)
                    timeout = TimeUnit.SECONDS.toMillis(definition.getTimeout());

                Transaction newTx = ignite.transactions().txStart(transactionConcurrency,
                    convertToIgniteIsolationLevel(definition.getIsolationLevel()), timeout, 0);

                if (log.isDebugEnabled())
                    log.debug("Started Ignite transaction: " + newTx);

                txObject.setTransactionHolder(new IgniteTransactionHolder(newTx), true);
            }

            txObject.getTransactionHolder().setSynchronizedWithTransaction(true);
            txObject.getTransactionHolder().setTransactionActive(true);

            tx = txObject.getTransactionHolder().getTransaction();

            // Bind the session holder to the thread.
            if (txObject.isNewTransactionHolder())
                TransactionSynchronizationManager.bindResource(this.ignite, txObject.getTransactionHolder());
        }
        catch (Exception ex) {
            if (tx != null)
                tx.close();

            throw new CannotCreateTransactionException("Could not create Ignite transaction", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        IgniteTransactionObject txObject = (IgniteTransactionObject)status.getTransaction();
        Transaction tx = txObject.getTransactionHolder().getTransaction();

        if (status.isDebug() && log.isDebugEnabled())
            log.debug("Committing Ignite transaction: " + tx);

        try {
            tx.commit();
        }
        catch (IgniteException e) {
            throw new TransactionSystemException("Could not commit Ignite transaction", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        IgniteTransactionObject txObject = (IgniteTransactionObject)status.getTransaction();
        Transaction tx = txObject.getTransactionHolder().getTransaction();

        if (status.isDebug() && log.isDebugEnabled())
            log.debug("Rolling back Ignite transaction: " + tx);

        try {
            tx.rollback();
        }
        catch (IgniteException e) {
            throw new TransactionSystemException("Could not rollback Ignite transaction", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void doCleanupAfterCompletion(Object transaction) {
        IgniteTransactionObject txObject = (IgniteTransactionObject)transaction;

        // Remove the transaction holder from the thread, if exposed.
        if (txObject.isNewTransactionHolder()) {
            Transaction tx = txObject.getTransactionHolder().getTransaction();
            TransactionSynchronizationManager.unbindResource(this.ignite);

            if (log.isDebugEnabled())
                log.debug("Releasing Ignite transaction: " + tx);
        }

        txObject.getTransactionHolder().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected boolean isExistingTransaction(Object transaction) throws TransactionException {
        IgniteTransactionObject txObject = (IgniteTransactionObject)transaction;

        return (txObject.getTransactionHolder() != null && txObject.getTransactionHolder().isTransactionActive());
    }

    /**
     * {@inheritDoc}
     */
    @Override public Object getResourceFactory() {
        return this.ignite;
    }

    /**
     * @param isolationLevel Spring isolation level.
     * @return Ignite isolation level.
     */
    private TransactionIsolation convertToIgniteIsolationLevel(int isolationLevel) {
        TransactionIsolation isolation = ignite.configuration().getTransactionConfiguration().getDefaultTxIsolation();
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

    /**
     * An object representing a managed Ignite transaction.
     */
    private static class IgniteTransactionObject {
        /** */
        private IgniteTransactionHolder transactionHolder;

        /** */
        private boolean newTransactionHolder;

        /**
         * Sets the resource holder being used to hold Ignite resources in the
         * transaction.
         *
         * @param transactionHolder the transaction resource holder
         * @param newHolder         true if the holder was created for this transaction,
         *                          false if it already existed
         */
        private void setTransactionHolder(IgniteTransactionHolder transactionHolder, boolean newHolder) {
            this.transactionHolder = transactionHolder;
            this.newTransactionHolder = newHolder;
        }

        /**
         * Returns the resource holder being used to hold Ignite resources in the
         * transaction.
         *
         * @return the transaction resource holder
         */
        private IgniteTransactionHolder getTransactionHolder() {
            return transactionHolder;
        }

        /**
         * Returns true if the transaction holder was created for the current
         * transaction and false if it existed prior to the transaction.
         *
         * @return true if the holder was created for this transaction, false if it
         * already existed
         */
        private boolean isNewTransactionHolder() {
            return newTransactionHolder;
        }
    }
}
