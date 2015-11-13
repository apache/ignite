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
 * Factory class to instantiate {@link CassandraCacheStore}.
 *
 * @param <K> Ignite cache key type
 * @param <V> Ignite cache value type
 */
public class CassandraCacheStoreFactory<K, V> implements Factory<CassandraCacheStore<K, V>> {
    /** TODO IGNITE-1371: add comment */
    @SpringApplicationContextResource
    private Object appCtx;

    /** TODO IGNITE-1371: add comment */
    private String dataSrcBean;

    /** TODO IGNITE-1371: add comment */
    private String persistenceSettingsBean;

    /** TODO IGNITE-1371: add comment */
    private transient DataSource dataSrc;

    /** TODO IGNITE-1371: add comment */
    private transient KeyValuePersistenceSettings persistenceSettings;

    /** {@inheritDoc} */
    @Override public CassandraCacheStore<K, V> create() {
        return new CassandraCacheStore<>(getDataSource(), getPersistenceSettings());
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public CassandraCacheStoreFactory<K, V> setDataSource(DataSource dataSrc) {
        this.dataSrc = dataSrc;
        return this;
    }

    /** TODO IGNITE-1371: add comment */
    public CassandraCacheStoreFactory<K, V> setDataSourceBean(String bean) {
        this.dataSrcBean = bean;
        return this;
    }

    /** TODO IGNITE-1371: add comment */
    @SuppressWarnings("UnusedDeclaration")
    public CassandraCacheStoreFactory<K, V> setPersistenceSettings(KeyValuePersistenceSettings settings) {
        this.persistenceSettings = settings;
        return this;
    }

    /** TODO IGNITE-1371: add comment */
    public CassandraCacheStoreFactory<K, V> setPersistenceSettingsBean(String bean) {
        this.persistenceSettingsBean = bean;
        return this;
    }

    /** TODO IGNITE-1371: add comment */
    private DataSource getDataSource() {
        if (dataSrc != null)
            return dataSrc;

        if (dataSrcBean == null)
            throw new IllegalStateException("Either DataSource bean or DataSource itself should be specified");

        if (appCtx == null) {
            throw new IllegalStateException("Can't get Cassandra DataSource cause Spring application " +
                "context wasn't injected into CassandraCacheStoreFactory");
        }

        Object obj = loadSpringContextBean(appCtx, dataSrcBean);

        if (!(obj instanceof DataSource))
            throw new IllegalStateException("Incorrect connection bean '" + dataSrcBean + "' specified");

        return dataSrc = (DataSource)obj;
    }

    /** TODO IGNITE-1371: add comment */
    private KeyValuePersistenceSettings getPersistenceSettings() {
        if (persistenceSettings != null)
            return persistenceSettings;

        if (persistenceSettingsBean == null) {
            throw new IllegalStateException("Either persistence settings bean or persistence settings itself " +
                "should be specified");
        }

        if (appCtx == null) {
            throw new IllegalStateException("Can't get Cassandra persistence settings cause Spring application " +
                "context wasn't injected into CassandraCacheStoreFactory");
        }

        Object obj = loadSpringContextBean(appCtx, persistenceSettingsBean);

        if (!(obj instanceof KeyValuePersistenceSettings)) {
            throw new IllegalStateException("Incorrect persistence settings bean '" +
                persistenceSettingsBean + "' specified");
        }

        return persistenceSettings = (KeyValuePersistenceSettings)obj;
    }

    /** TODO IGNITE-1371: add comment */
    private Object loadSpringContextBean(Object appCtx, String beanName) {
        try {
            IgniteSpringHelper spring = IgniteComponentType.SPRING.create(false);
            return spring.loadBeanFromAppContext(appCtx, beanName);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to load bean in application context [beanName=" + beanName + ", igniteConfig=" + appCtx + ']', e);
        }
    }

}
