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
import org.apache.ignite.cache.store.cassandra.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
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
    /** */
    private static final long serialVersionUID = 0L;

    /** Auto-injected Spring ApplicationContext resource. */
    @SpringApplicationContextResource
    private Object appCtx;

    /** Name of data source bean. */
    private String dataSrcBean;

    /** Name of persistence settings bean. */
    private String persistenceSettingsBean;

    /** Data source. */
    private DataSource dataSrc;

    /** Persistence settings. */
    private KeyValuePersistenceSettings persistenceSettings;

    /** Max workers thread count. These threads are responsible for load cache. */
    private int maxPoolSize = Runtime.getRuntime().availableProcessors();

    /** {@inheritDoc} */
    @Override public CassandraCacheStore<K, V> create() {
        return new CassandraCacheStore<>(getDataSource(), getPersistenceSettings(), getMaxPoolSize());
    }

    /**
     * Sets data source.
     *
     * @param dataSrc Data source.
     *
     * @return {@code This} for chaining.
     */
    public CassandraCacheStoreFactory<K, V> setDataSource(DataSource dataSrc) {
        this.dataSrc = dataSrc;

        return this;
    }

    /**
     * Sets data source bean name.
     *
     * @param beanName Data source bean name.
     * @return {@code This} for chaining.
     */
    public CassandraCacheStoreFactory<K, V> setDataSourceBean(String beanName) {
        this.dataSrcBean = beanName;

        return this;
    }

    /**
     * Sets persistence settings.
     *
     * @param settings Persistence settings.
     * @return {@code This} for chaining.
     */
    public CassandraCacheStoreFactory<K, V> setPersistenceSettings(KeyValuePersistenceSettings settings) {
        this.persistenceSettings = settings;

        return this;
    }

    /**
     * Sets persistence settings bean name.
     *
     * @param beanName Persistence settings bean name.
     * @return {@code This} for chaining.
     */
    public CassandraCacheStoreFactory<K, V> setPersistenceSettingsBean(String beanName) {
        this.persistenceSettingsBean = beanName;

        return this;
    }

    /**
     * @return Data source.
     */
    private DataSource getDataSource() {
        if (dataSrc != null)
            return dataSrc;

        if (dataSrcBean == null)
            throw new IllegalStateException("Either DataSource bean or DataSource itself should be specified");

        if (appCtx == null) {
            throw new IllegalStateException("Failed to get Cassandra DataSource cause Spring application " +
                "context wasn't injected into CassandraCacheStoreFactory");
        }

        Object obj = loadSpringContextBean(appCtx, dataSrcBean);

        if (!(obj instanceof DataSource))
            throw new IllegalStateException("Incorrect connection bean '" + dataSrcBean + "' specified");

        return dataSrc = (DataSource)obj;
    }

    /**
     * @return Persistence settings.
     */
    private KeyValuePersistenceSettings getPersistenceSettings() {
        if (persistenceSettings != null)
            return persistenceSettings;

        if (persistenceSettingsBean == null) {
            throw new IllegalStateException("Either persistence settings bean or persistence settings itself " +
                "should be specified");
        }

        if (appCtx == null) {
            throw new IllegalStateException("Failed to get Cassandra persistence settings cause Spring application " +
                "context wasn't injected into CassandraCacheStoreFactory");
        }

        Object obj = loadSpringContextBean(appCtx, persistenceSettingsBean);

        if (!(obj instanceof KeyValuePersistenceSettings)) {
            throw new IllegalStateException("Incorrect persistence settings bean '" +
                persistenceSettingsBean + "' specified");
        }

        return persistenceSettings = (KeyValuePersistenceSettings)obj;
    }

    /**
     * Get maximum workers thread count. These threads are responsible for queries execution.
     *
     * @return Maximum workers thread count.
     */
    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    /**
     * Set Maximum workers thread count. These threads are responsible for queries execution.
     *
     * @param maxPoolSize Max workers thread count.
     * @return {@code This} for chaining.
     */
    public CassandraCacheStoreFactory<K, V> setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;

        return this;
    }

    /**
     * Loads bean from Spring ApplicationContext.
     *
     * @param appCtx Application context.
     * @param beanName Bean name to load.
     * @return Loaded bean.
     */
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
