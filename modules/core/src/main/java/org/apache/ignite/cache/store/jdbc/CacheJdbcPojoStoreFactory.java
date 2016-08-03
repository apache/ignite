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

package org.apache.ignite.cache.store.jdbc;

import javax.cache.configuration.Factory;
import javax.sql.DataSource;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.jdbc.dialect.JdbcDialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.SpringApplicationContextResource;

/**
 * {@link Factory} implementation for {@link CacheJdbcPojoStore}.
 *
 * Use this factory to pass {@link CacheJdbcPojoStore} to {@link CacheConfiguration}.
 * <p>
 * Please note, that {@link CacheJdbcPojoStoreFactory#setDataSource(DataSource)} is deprecated and
 * {@link CacheJdbcPojoStoreFactory#setDataSourceFactory(Factory)} or
 * {@link CacheJdbcPojoStoreFactory#setDataSourceBean(String)} should be used instead.
 *
 * <h2 class="header">Spring Example</h2>
 * <pre name="code" class="xml">
 *     &lt;bean id= "myDataSource" class="org.h2.jdbcx.JdbcDataSource"/&gt;
 *
 *     &lt;bean id="ignite.cfg" class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
 *          ...
 *          &lt;property name="cacheConfiguration"&gt;
 *               &lt;list&gt;
 *                  &lt;bean class="org.apache.ignite.configuration.CacheConfiguration"&gt;
 *                      &lt;property name="name" value="myCache" /&gt;
 *                      ...
 *                      &lt;property name="cacheStoreFactory"&gt;
 *                          &lt;bean class="org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory"&gt;
 *                              &lt;property name="dataSourceBean" value="myDataSource" /&gt;
 *                              &lt;property name="types"&gt;
 *                                  &lt;list&gt;
 *                                      &lt;bean class="org.apache.ignite.cache.store.jdbc.JdbcType"&gt;
 *                                          &lt;property name="cacheName" value="myCache" /&gt;
 *                                          &lt;property name="databaseSchema" value="MY_DB_SCHEMA" /&gt;
 *                                          &lt;property name="databaseTable" value="PERSON" /&gt;
 *                                          &lt;property name="keyType" value="java.lang.Integer" /&gt;
 *                                          &lt;property name="keyFields"&gt;
 *                                              &lt;list&gt;
 *                                                  &lt;bean class="org.apache.ignite.cache.store.jdbc.JdbcTypeField"&gt;
 *                                                      &lt;property name="databaseFieldType" &gt;
 *                                                          &lt;util:constant static-field="java.sql.Types.INTEGER"/&gt;
 *                                                      &lt;/property&gt;
 *                                                      &lt;property name="databaseFieldName" value="ID" /&gt;
 *                                                      &lt;property name="javaFieldType" value="java.lang.Integer" /&gt;
 *                                                      &lt;property name="javaFieldName" value="id" /&gt;
 *                                                  &lt;/bean&gt;
 *                                              &lt;/list&gt;
 *                                          &lt;/property&gt;
 *                                          &lt;property name="valueType" value="my.company.Person" /&gt;
 *                                          &lt;property name="valueFields"&gt;
 *                                              &lt;list&gt;
 *                                                  &lt;bean class="org.apache.ignite.cache.store.jdbc.JdbcTypeField"&gt;
 *                                                      &lt;property name="databaseFieldType" &gt;
 *                                                          &lt;util:constant static-field="java.sql.Types.VARCHAR"/&gt;
 *                                                      &lt;/property&gt;
 *                                                      &lt;property name="databaseFieldName" value="NAME" /&gt;
 *                                                      &lt;property name="javaFieldType" value="java.lang.String" /&gt;
 *                                                      &lt;property name="javaFieldName" value="name" /&gt;
 *                                                  &lt;/bean&gt;
 *                                              &lt;/list&gt;
 *                                          &lt;/property&gt;
 *                                      &lt;/bean&gt;
 *                                  &lt;/list&gt;
 *                              &lt;/property&gt;
 *                          &lt;/bean&gt;
 *                      &lt;/property&gt;
 *                  &lt;/bean&gt;
 *               &lt;/list&gt;
 *          &lt;/property&gt;
 *     &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
public class CacheJdbcPojoStoreFactory<K, V> implements Factory<CacheAbstractJdbcStore<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default value for write attempts. */
    public static final int DFLT_WRITE_ATTEMPTS = 2;

    /** Default batch size for put and remove operations. */
    public static final int DFLT_BATCH_SIZE = 512;

    /** Default batch size for put and remove operations. */
    public static final int DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD = 512;

    /** Maximum batch size for writeAll and deleteAll operations. */
    private int batchSizw = DFLT_BATCH_SIZE;

    /** Name of data source bean. */
    private String dataSrcBean;

    /** Database dialect. */
    private JdbcDialect dialect;

    /** Max workers thread count. These threads are responsible for load cache. */
    private int maxPoolSize = Runtime.getRuntime().availableProcessors();

    /** Maximum write attempts in case of database error. */
    private int maxWriteAttempts = DFLT_WRITE_ATTEMPTS;

    /** Parallel load cache minimum threshold. If {@code 0} then load sequentially. */
    private int parallelLoadCacheMinThreshold = DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD;

    /** Hash calculator.  */
    private JdbcTypeHasher hasher = JdbcTypeDefaultHasher.INSTANCE;

    /** Types transformer.  */
    private JdbcTypesTransformer transformer = JdbcTypesDefaultTransformer.INSTANCE;

    /** Types that store could process. */
    private JdbcType[] types;

    /** Data source. */
    private transient DataSource dataSrc;

    /** Data source factory. */
    private Factory<DataSource> dataSrcFactory;

    /** Application context. */
    @SpringApplicationContextResource
    private transient Object appCtx;

    /** {@inheritDoc} */
    @Override public CacheJdbcPojoStore<K, V> create() {
        CacheJdbcPojoStore<K, V> store = new CacheJdbcPojoStore<>();

        store.setBatchSize(batchSizw);
        store.setDialect(dialect);
        store.setMaximumPoolSize(maxPoolSize);
        store.setMaximumWriteAttempts(maxWriteAttempts);
        store.setParallelLoadCacheMinimumThreshold(parallelLoadCacheMinThreshold);
        store.setTypes(types);
        store.setHasher(hasher);
        store.setTransformer(transformer);

        if (dataSrc != null)
            store.setDataSource(dataSrc);
        else if (dataSrcBean != null) {
            if (appCtx == null)
                throw new IgniteException("Spring application context resource is not injected.");

            IgniteSpringHelper spring;

            try {
                spring = IgniteComponentType.SPRING.create(false);

                DataSource data = spring.loadBeanFromAppContext(appCtx, dataSrcBean);

                store.setDataSource(data);
            }
            catch (Exception e) {
                throw new IgniteException("Failed to load bean in application context [beanName=" + dataSrcBean +
                    ", igniteConfig=" + appCtx + ']', e);
            }
        }
        else if (dataSrcFactory != null)
            store.setDataSource(dataSrcFactory.create());

        return store;
    }

    /**
     * Sets data source. Data source should be fully configured and ready-to-use.
     *
     * @param dataSrc Data source.
     * @return {@code This} for chaining.
     * @see CacheJdbcPojoStore#setDataSource(DataSource)
     */
    @Deprecated
    public CacheJdbcPojoStoreFactory<K, V> setDataSource(DataSource dataSrc) {
        this.dataSrc = dataSrc;

        return this;
    }

    /**
     * Get maximum batch size for delete and delete operations.
     *
     * @return Maximum batch size.
     */
    public int getBatchSize() {
        return batchSizw;
    }

    /**
     * Set maximum batch size for write and delete operations.
     *
     * @param batchSize Maximum batch size.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreFactory setBatchSize(int batchSize) {
        this.batchSizw = batchSize;

        return this;
    }

    /**
     * Gets name of the data source bean.
     *
     * @return Data source bean name.
     */
    public String getDataSourceBean() {
        return dataSrcBean;
    }

    /**
     * Sets name of the data source bean.
     *
     * @param dataSrcBean Data source bean name.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreFactory setDataSourceBean(String dataSrcBean) {
        this.dataSrcBean = dataSrcBean;

        return this;
    }

    /**
     * Get database dialect.
     *
     * @return Database dialect.
     */
    public JdbcDialect getDialect() {
        return dialect;
    }

    /**
     * Set database dialect.
     *
     * @param dialect Database dialect.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreFactory setDialect(JdbcDialect dialect) {
        this.dialect = dialect;

        return this;
    }

    /**
     * Get maximum workers thread count. These threads are responsible for queries execution.
     *
     * @return Maximum workers thread count.
     */
    public int getMaximumPoolSize() {
        return maxPoolSize;
    }

    /**
     * Set Maximum workers thread count. These threads are responsible for queries execution.
     *
     * @param maxPoolSize Max workers thread count.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreFactory setMaximumPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;

        return this;
    }

    /**
     * Gets maximum number of write attempts in case of database error.
     *
     * @return Maximum number of write attempts.
     */
    public int getMaximumWriteAttempts() {
        return maxWriteAttempts;
    }

    /**
     * Sets maximum number of write attempts in case of database error.
     *
     * @param maxWrtAttempts Number of write attempts.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreFactory setMaximumWriteAttempts(int maxWrtAttempts) {
        this.maxWriteAttempts = maxWrtAttempts;

        return this;
    }

    /**
     * Parallel load cache minimum row count threshold.
     *
     * @return If {@code 0} then load sequentially.
     */
    public int getParallelLoadCacheMinimumThreshold() {
        return parallelLoadCacheMinThreshold;
    }

    /**
     * Parallel load cache minimum row count threshold.
     *
     * @param parallelLoadCacheMinThreshold Minimum row count threshold. If {@code 0} then load sequentially.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreFactory setParallelLoadCacheMinimumThreshold(int parallelLoadCacheMinThreshold) {
        this.parallelLoadCacheMinThreshold = parallelLoadCacheMinThreshold;

        return this;
    }

    /**
     * Gets types known by store.
     *
     * @return Types known by store.
     */
    public JdbcType[] getTypes() {
        return types;
    }

    /**
     * Sets store configurations.
     *
     * @param types Store should process.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreFactory setTypes(JdbcType... types) {
        this.types = types;

        return this;
    }

    /**
     * Gets hash code calculator.
     *
     * @return Hash code calculator.
     */
    public JdbcTypeHasher getHasher() {
        return hasher;
    }

    /**
     * Sets hash code calculator.
     *
     * @param hasher Hash code calculator.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreFactory setHasher(JdbcTypeHasher hasher) {
        this.hasher = hasher;

        return this;
    }

    /**
     * Gets types transformer.
     *
     * @return Types transformer.
     */
    public JdbcTypesTransformer getTransformer() {
        return transformer;
    }

    /**
     * Sets types transformer.
     *
     * @param transformer Types transformer.
     * @return {@code This} for chaining.
     */
    public CacheJdbcPojoStoreFactory setTransformer(JdbcTypesTransformer transformer) {
        this.transformer = transformer;

        return this;
    }

    /**
     * Gets factory for underlying datasource.
     *
     * @return Cache store factory.
     */
    @SuppressWarnings("unchecked")
    public Factory<DataSource> getDataSourceFactory() {
        return dataSrcFactory;
    }

    /**
     * Sets factory for underlying datasource.

     * @param dataSrcFactory Datasource factory.
     * @return {@code this} for chaining.
     */
    @SuppressWarnings("unchecked")
    public CacheJdbcPojoStoreFactory<K, V> setDataSourceFactory(Factory<DataSource> dataSrcFactory) {
        this.dataSrcFactory = dataSrcFactory;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheJdbcPojoStoreFactory.class, this);
    }
}
