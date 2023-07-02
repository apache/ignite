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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSpring;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.transactions.proxy.IgniteTransactionProxyFactory;
import org.apache.ignite.internal.transactions.proxy.TransactionProxyFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.event.ContextRefreshedEvent;

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
 * simply provide correct Ignite instance name, like below (if there is no Grid
 * instance with such name, exception will be thrown):
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:tx="http://www.springframework.org/schema/tx"
 *        xsi:schemaLocation="
 *            http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *            http://www.springframework.org/schema/tx http://www.springframework.org/schema/tx/spring-tx.xsd"&gt;
 *     &lt;-- Provide Ignite instance name. --&gt;
 *     &lt;bean id="transactionManager" class="org.apache.ignite.transactions.spring.SpringTransactionManager"&gt;
 *         &lt;property name="igniteInstanceName" value="myGrid"/&gt;
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
 * {@link #setIgniteInstanceName(String) igniteInstanceName} are provided, transaction manager
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
 *     &lt;-- Provide Ignite instance name. --&gt;
 *     &lt;bean id="transactionManager" class="org.apache.ignite.transactions.spring.SpringTransactionManager"&gt;
 *         &lt;property name="igniteInstanceName" value="myGrid"/&gt;
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
 *         &lt;property name="igniteInstanceName" value="myGrid"/&gt;
 *         &lt;property name="transactionConcurrency" value="OPTIMISTIC"/&gt;
 *     &lt;/bean>
 *
 *     &lt;bean id="pessimisticTransactionManager" class="org.apache.ignite.transactions.spring.SpringTransactionManager"&gt;
 *         &lt;property name="igniteInstanceName" value="myGrid"/&gt;
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
public class SpringTransactionManager extends AbstractSpringTransactionManager implements ApplicationContextAware,
    DisposableBean {
    /** Grid configuration file path. */
    private String cfgPath;

    /** Ignite configuration. */
    private IgniteConfiguration cfg;

    /** Ignite instance name. */
    private String igniteInstanceName;

    /** Ignite instance. */
    private Ignite ignite;

    /** Flag indicating that Ignite instance was not created inside current transaction manager. */
    private boolean externalIgniteInstance;

    /** Ignite transactions configuration. */
    private TransactionConfiguration txCfg;

    /** Spring context */
    private ApplicationContext springCtx;

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
     * @deprecated Use {@link #getIgniteInstanceName()}.
     */
    @Deprecated
    public String getGridName() {
        return getIgniteInstanceName();
    }

    /**
     * Sets grid name.
     *
     * @param gridName Grid name.
     * @deprecated Use {@link #setIgniteInstanceName(String)}.
     */
    @Deprecated
    public void setGridName(String gridName) {
        setIgniteInstanceName(gridName);
    }

    /**
     * Gets Ignite instance name.
     *
     * @return Ignite instance name.
     */
    public String getIgniteInstanceName() {
        return igniteInstanceName;
    }

    /**
     * Sets Ignite instance name.
     *
     * @param igniteInstanceName Ignite instance name.
     */
    public void setIgniteInstanceName(String igniteInstanceName) {
        this.igniteInstanceName = igniteInstanceName;
    }

    /** {@inheritDoc} */
    @Override public void onApplicationEvent(ContextRefreshedEvent evt) {
        if (ignite == null) {
            if (cfgPath != null && cfg != null) {
                throw new IllegalArgumentException("Both 'configurationPath' and 'configuration' are " +
                    "provided. Set only one of these properties if you need to start a Ignite node inside of " +
                    "SpringTransactionManager. If you already have a node running, omit both of them and set" +
                    "'igniteInstanceName' property.");
            }

            try {
                if (cfgPath != null)
                    ignite = IgniteSpring.start(cfgPath, springCtx);
                else if (cfg != null)
                    ignite = IgniteSpring.start(cfg, springCtx);
                else {
                    ignite = Ignition.ignite(igniteInstanceName);

                    externalIgniteInstance = true;
                }
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }

            txCfg = ignite.configuration().getTransactionConfiguration();
        }

        super.onApplicationEvent(evt);
    }

    /** {@inheritDoc} */
    @Override public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.springCtx = ctx;
    }

    /** {@inheritDoc} */
    @Override protected TransactionIsolation defaultTransactionIsolation() {
        return txCfg.getDefaultTxIsolation();
    }

    /** {@inheritDoc} */
    @Override protected long defaultTransactionTimeout() {
        return txCfg.getDefaultTxTimeout();
    }

    /** {@inheritDoc} */
    @Override protected IgniteLogger log() {
        return ignite.log();
    }

    /** {@inheritDoc} */
    @Override protected TransactionConcurrency defaultTransactionConcurrency() {
        return txCfg.getDefaultTxConcurrency();
    }

    /** {@inheritDoc} */
    @Override protected TransactionProxyFactory createTransactionFactory() {
        return new IgniteTransactionProxyFactory(ignite.transactions());
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        if (!externalIgniteInstance)
            ignite.close();
    }
}
