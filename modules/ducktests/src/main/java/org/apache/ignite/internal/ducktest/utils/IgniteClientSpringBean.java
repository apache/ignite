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
package org.apache.ignite.internal.ducktest.utils;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.*;
import org.apache.ignite.configuration.ClientConfiguration;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.event.ContextRefreshedEvent;

import java.util.Collection;
import java.util.List;

/**
 * Represents Ignite client Spring bean that provides the ability to automatically start Ignite client during
 * Spring Context initialization. It requires {@link ClientConfiguration} to be set before bean use
 * (see {@link #setClientConfiguration(ClientConfiguration)}}).
 *
 * A note should be taken that Ignite client instance is started after all other
 * Spring beans have been initialized and right before Spring context is refreshed.
 * That implies that it's not valid to reference {@link IgniteClientSpringBean} from
 * any kind of Spring bean init methods like {@link javax.annotation.PostConstruct}.
 * If it's required to reference IgniteSpringBean for other bean
 * initialization purposes, it should be done from a {@link ContextRefreshedEvent}
 * listener method declared in that bean.
 *
 * <h3 class="header">Spring XML Configuration Example</h3>
 * <pre name="code" class="xml">
 * &lt;bean id="igniteClient" class="org.apache.ignite.IgniteClientSpringBean"&gt;
 *     &lt;property name="clientConfiguration"&gt;
 *         &lt;bean class="org.apache.ignite.configuration.ClientConfiguration"&gt;
 *             &lt;property name="addresses"&gt;
 *                 &lt;list&gt;
 *                     &lt;value&gt;127.0.0.1:10800&lt;/value&gt;
 *                 &lt;/list&gt;
 *             &lt;/property&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 */
public class IgniteClientSpringBean implements IgniteClient, SmartInitializingSingleton {
    /** Ignite client instance to which operations are delegated. */
    private IgniteClient cli;

    /** Ignite client configuration. */
    private ClientConfiguration cfg;

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> getOrCreateCache(String name) throws ClientException {
        return cli.getOrCreateCache(name);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteClientFuture<ClientCache<K, V>> getOrCreateCacheAsync(String name)
            throws ClientException
    {
        return cli.getOrCreateCacheAsync(name);
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> getOrCreateCache(ClientCacheConfiguration cfg) throws ClientException {
        return cli.getOrCreateCache(cfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteClientFuture<ClientCache<K, V>> getOrCreateCacheAsync(ClientCacheConfiguration cfg)
            throws ClientException
    {
        return cli.getOrCreateCacheAsync(cfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> cache(String name) {
        return cli.cache(name);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() throws ClientException {
        return cli.cacheNames();
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Collection<String>> cacheNamesAsync() throws ClientException {
        return cli.cacheNamesAsync();
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String name) throws ClientException {
        cli.destroyCache(name);
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Void> destroyCacheAsync(String name) throws ClientException {
        return cli.destroyCacheAsync(name);
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> createCache(String name) throws ClientException {
        return cli.createCache(name);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteClientFuture<ClientCache<K, V>> createCacheAsync(String name) throws ClientException {
        return cli.createCacheAsync(name);
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClientCache<K, V> createCache(ClientCacheConfiguration cfg) throws ClientException {
        return cli.createCache(cfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteClientFuture<ClientCache<K, V>> createCacheAsync(ClientCacheConfiguration cfg)
            throws ClientException
    {
        return cli.createCacheAsync(cfg);
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        return cli.binary();
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
        return cli.query(qry);
    }

    /** {@inheritDoc} */
    @Override public ClientTransactions transactions() {
        return cli.transactions();
    }

    /** {@inheritDoc} */
    @Override public ClientCompute compute() {
        return cli.compute();
    }

    /** {@inheritDoc} */
    @Override public ClientCompute compute(ClientClusterGroup grp) {
        return cli.compute(grp);
    }

    /** {@inheritDoc} */
    @Override public ClientCluster cluster() {
        return cli.cluster();
    }

    /** {@inheritDoc} */
    @Override public ClientServices services() {
        return cli.services();
    }

    /** {@inheritDoc} */
    @Override public ClientServices services(ClientClusterGroup grp) {
        return cli.services(grp);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        cli.close();
    }

    /** {@inheritDoc} */
    @Override public void afterSingletonsInstantiated() {
        if (cfg == null)
            throw new IllegalArgumentException("Ignite client configuration must be set.");

        cli = Ignition.startClient(cfg);
    }

    /** Sets Ignite client configuration. */
    public IgniteClientSpringBean setClientConfiguration(ClientConfiguration cfg) {
        this.cfg = cfg;

        return this;
    }

    /** Gets Ignite client configuration. */
    public ClientConfiguration getClientConfiguration() {
        return cfg;
    }
}