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

package org.apache.ignite.cache.store.cassandra;

import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.cassandra.utils.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.utils.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.resources.SpringApplicationContextResource;

/**
 * Factory class to instantiate ${@link org.apache.ignite.cache.store.cassandra.CassandraCacheStore}
 * @param <K> Ignite cache key type
 * @param <V> Ignite cache value type
 */
public class CassandraCacheStoreFactory<K, V> implements Factory<CassandraCacheStore<K, V>> {
    @SpringApplicationContextResource
    private Object appContext;

    private String dataSourceBean;
    private String persistenceSettingsBean;

    private transient DataSource dataSource;
    private transient KeyValuePersistenceSettings persistenceSettings;

    @Override public CassandraCacheStore<K, V> create() {
        return new CassandraCacheStore<>(getDataSource(), getPersistenceSettings());
    }

    @SuppressWarnings("UnusedDeclaration")
    public CassandraCacheStoreFactory<K, V> setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    public CassandraCacheStoreFactory<K, V> setDataSourceBean(String bean) {
        this.dataSourceBean = bean;
        return this;
    }

    @SuppressWarnings("UnusedDeclaration")
    public CassandraCacheStoreFactory<K, V> setPersistenceSettings(KeyValuePersistenceSettings settings) {
        this.persistenceSettings = settings;
        return this;
    }

    public CassandraCacheStoreFactory<K, V> setPersistenceSettingsBean(String bean) {
        this.persistenceSettingsBean = bean;
        return this;
    }

    private DataSource getDataSource() {
        if (dataSource != null)
            return dataSource;

        if (dataSourceBean == null)
            throw new IllegalStateException("Either DataSource bean or DataSource itself should be specified");

        if (appContext == null) {
            throw new IllegalStateException("Can't get Cassandra DataSource cause Spring application " +
                "context wasn't injected into CassandraCacheStoreFactory");
        }

        Object obj = loadSpringContextBean(appContext, dataSourceBean);

        if (!(obj instanceof DataSource))
            throw new IllegalStateException("Incorrect connection bean '" + dataSourceBean + "' specified");

        return dataSource = (DataSource)obj;
    }

    private KeyValuePersistenceSettings getPersistenceSettings() {
        if (persistenceSettings != null)
            return persistenceSettings;

        if (persistenceSettingsBean == null) {
            throw new IllegalStateException("Either persistence settings bean or persistence settings itself " +
                "should be specified");
        }

        if (appContext == null) {
            throw new IllegalStateException("Can't get Cassandra persistence settings cause Spring application " +
                "context wasn't injected into CassandraCacheStoreFactory");
        }

        Object obj = loadSpringContextBean(appContext, persistenceSettingsBean);

        if (!(obj instanceof KeyValuePersistenceSettings)) {
            throw new IllegalStateException("Incorrect persistence settings bean '" +
                persistenceSettingsBean + "' specified");
        }

        return persistenceSettings = (KeyValuePersistenceSettings)obj;
    }

    private Object loadSpringContextBean(Object appContext, String beanName) {
        try {
            IgniteSpringHelper spring = IgniteComponentType.SPRING.create(false);
            return spring.loadBeanFromAppContext(appContext, beanName);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to load bean in application context [beanName=" + beanName + ", igniteConfig=" + appContext + ']', e);
        }
    }

}
