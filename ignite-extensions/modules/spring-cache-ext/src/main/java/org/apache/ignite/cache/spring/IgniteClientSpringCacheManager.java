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

package org.apache.ignite.cache.spring;

import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.springdata.proxy.IgniteClientCacheProxy;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * Represents implementation of {@link AbstractCacheManager} that uses thin client to connect to an Ignite cluster
 * and obtain an Ignite cache instance. It requires {@link IgniteClient} instance or {@link ClientConfiguration} to be
 * set before manager use (see {@link #setClientInstance(IgniteClient),
 * {@link #setClientConfiguration(ClientConfiguration)}}).
 *
 * Note that current manager implementation can be used only with Ignite since 2.11.0 version.
 * For Ignite versions earlier than 2.11.0 use an {@link SpringCacheManager}.
 *
 * Note that Spring Cache synchronous mode ({@link Cacheable#sync}) is not supported by the current manager
 * implementation. Instead, use an {@link SpringCacheManager} that uses an Ignite thick client to connect to Ignite cluster.
 *
 * You can provide Ignite client instance to a Spring configuration XML file, like below:
 *
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:cache="http://www.springframework.org/schema/cache"
 *        xsi:schemaLocation="
 *            http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *            http://www.springframework.org/schema/cache http://www.springframework.org/schema/cache/spring-cache.xsd"&gt;
 *     &lt;!--
 *              Note that org.apache.ignite.IgniteClientSpringBean is available since Ignite 2.11.0 version.
 *              For Ignite versions earlier than 2.11.0 org.apache.ignite.client.IgniteClient bean should be created
 *              manually with concern of its connection to the Ignite cluster.
 *     --&gt;
 *     &lt;bean id="igniteClient" class="org.apache.ignite.IgniteClientSpringBean"&gt;
 *         &lt;property name="clientConfiguration"&gt;
 *             &lt;bean class="org.apache.ignite.configuration.ClientConfiguration"&gt;
 *             &lt;property name="addresses"&gt;
 *                 &lt;list&gt;
 *                     &lt;value&gt;127.0.0.1:10800&lt;/value&gt;
 *                 &lt;/list&gt;
 *             &lt;/property&gt;
 *         &lt;/bean&gt;
 *         &lt;/property&gt;
 *     &lt;/bean>
 *
 *     &lt;!-- Provide Ignite client instance. --&gt;
 *     &lt;bean id="cacheManager" class="org.apache.ignite.cache.spring.IgniteClientSpringCacheManager"&gt;
 *         &lt;property name="clientInstance" ref="igniteClient"/&gt;
 *     &lt;/bean>
 *
 *     &lt;!-- Use annotation-driven cache configuration. --&gt;
 *     &lt;cache:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 *
 * Or you can provide Ignite client configuration, like below:
 *
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:cache="http://www.springframework.org/schema/cache"
 *        xsi:schemaLocation="
 *            http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *            http://www.springframework.org/schema/cache http://www.springframework.org/schema/cache/spring-cache.xsd"&gt;
 *     &lt;!-- Provide Ignite client instance. --&gt;
 *     &lt;bean id="cacheManager" class="org.apache.ignite.cache.spring.IgniteClientSpringCacheManager"&gt;
 *         &lt;property name="clientConfiguration"&gt;
 *             &lt;bean class="org.apache.ignite.configuration.ClientConfiguration"&gt;
 *             &lt;property name="addresses"&gt;
 *                 &lt;list&gt;
 *                     &lt;value&gt;127.0.0.1:10800&lt;/value&gt;
 *                 &lt;/list&gt;
 *             &lt;/property&gt;
 *         &lt;/bean&gt;
 *         &lt;/property&gt;
 *     &lt;/bean>
 *
 *     &lt;!-- Use annotation-driven cache configuration. --&gt;
 *     &lt;cache:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 */
public class IgniteClientSpringCacheManager extends AbstractCacheManager implements DisposableBean,
    ApplicationListener<ContextRefreshedEvent> {
    /** Ignite client instance. */
    private IgniteClient cli;

    /** Ignite client configuration. */
    private ClientConfiguration cliCfg;

    /** Dynamic Ignite cache configuration template. */
    private ClientCacheConfiguration dynamicCacheCfg;

    /** Flag that indicates whether Ignite client instance was set explicitly. */
    private boolean externalCliInstance;

    /** @return Gets Ignite client instance. */
    public IgniteClient getClientInstance() {
        return cli;
    }

    /**
     * @param cli Ignite client instance.
     * @return Ignite Client Cache Manager.
     */
    public IgniteClientSpringCacheManager setClientInstance(IgniteClient cli) {
        A.notNull(cli, "cli");

        this.cli = cli;

        externalCliInstance = true;

        return this;
    }

    /** @return Gets Ignite client configuration. */
    public ClientConfiguration getClientConfiguration() {
        return cliCfg;
    }

    /**
     * @param cliCfg Ignite client configuration that will be used to start the client instance by the manager.
     * @return Ignite Client Cache Manager.
     */
    public IgniteClientSpringCacheManager setClientConfiguration(ClientConfiguration cliCfg) {
        this.cliCfg = cliCfg;

        return this;
    }

    /** @return Gets dynamic Ignite cache configuration template. */
    public ClientCacheConfiguration getDynamicCacheConfiguration() {
        return dynamicCacheCfg;
    }

    /**
     * Sets the Ignite cache configurations template that will be used to start the Ignite cache with the name
     * requested by the Spring Cache Framework if one does not exist. Note that provided cache name will be erased and
     * replaced with requested one.
     *
     * @param dynamicCacheCfg Client configuration.
     * @return Ignite Spring Cache Manager.
     */
    public IgniteClientSpringCacheManager setDynamicCacheConfiguration(ClientCacheConfiguration dynamicCacheCfg) {
        this.dynamicCacheCfg = dynamicCacheCfg;

        return this;
    }

    /** {@inheritDoc} */
    @Override protected SpringCache createCache(String name) {
        ClientCacheConfiguration ccfg = dynamicCacheCfg == null
            ? new ClientCacheConfiguration()
            : new ClientCacheConfiguration(dynamicCacheCfg);

        ClientCache<Object, Object> cache = cli.getOrCreateCache(ccfg.setName(name));

        return new SpringCache(new IgniteClientCacheProxy<>(cache), this);
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws Exception {
        if (!externalCliInstance)
            cli.close();
    }

    /** {@inheritDoc} */
    @Override protected Lock getSyncLock(String cache, Object key) {
        throw new UnsupportedOperationException("Synchronous mode is not supported for the Ignite Spring Cache" +
            " implementation that uses a thin client to connecting to an Ignite cluster.");
    }

    /** {@inheritDoc} */
    @Override public void onApplicationEvent(ContextRefreshedEvent evt) {
        if (cli != null)
            return;

        if (cliCfg == null) {
            throw new IllegalArgumentException("Neither client instance nor client configuration is specified." +
                " Set the 'clientInstance' property if you already have an Ignite client instance running," +
                " or set the 'clientConfiguration' property if an Ignite client instance need to be started" +
                " implicitly by the manager.");
        }

        cli = Ignition.startClient(cliCfg);
    }
}
