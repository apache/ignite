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
 *
 * <h2 class="header">Spring Example</h2>
 * <pre name="code" class="xml">
 *     &lt;bean id= "simpleDataSource" class="org.h2.jdbcx.JdbcDataSource"/&gt;
 *
 *     &lt;bean id="ignite.cfg" class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
 *          ...
 *          &lt;property name="cacheConfiguration"&gt;
 *               &lt;list&gt;
 *                  &lt;bean class="org.apache.ignite.configuration.CacheConfiguration"&gt;
 *                      ...
 *                      &lt;property name="cacheStoreFactory"&gt;
 *                          &lt;bean class="org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory"&gt;
 *                              &lt;property name="dataSourceBean" value = "simpleDataSource" /&gt;
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
public class CacheJdbcPojoStoreFactory<K, V> implements Factory<CacheJdbcPojoStore<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Name of data source bean. */
    private String dataSrcBean;

    /** Data source. */
    private transient DataSource dataSrc;

    /** Database dialect. */
    private JdbcDialect dialect;

    /** Application context. */
    @SpringApplicationContextResource
    private transient Object appContext;

    /** {@inheritDoc} */
    @Override public CacheJdbcPojoStore<K, V> create() {
        CacheJdbcPojoStore<K, V> store = new CacheJdbcPojoStore<>();

        store.setDialect(dialect);

        if (dataSrc != null)
            store.setDataSource(dataSrc);
        else if (dataSrcBean != null) {
            if (appContext == null)
                throw new IgniteException("Spring application context resource is not injected.");

            IgniteSpringHelper spring;

            try {
                spring = IgniteComponentType.SPRING.create(false);

                DataSource data = spring.loadBeanFromAppContext(appContext, dataSrcBean);

                store.setDataSource(data);
            }
            catch (Exception e) {
                throw new IgniteException("Failed to load bean in application context [beanName=" + dataSrcBean +
                    ", igniteConfig=" + appContext + ']', e);
            }
        }

        return store;
    }

    /**
     * Sets name of the data source bean.
     *
     * @param dataSrcBean Data source bean name.
     * @return {@code This} for chaining.
     * @see CacheJdbcPojoStore#setDataSource(DataSource)
     */
    public CacheJdbcPojoStoreFactory<K, V> setDataSourceBean(String dataSrcBean) {
        this.dataSrcBean = dataSrcBean;

        return this;
    }

    /**
     * Sets data source. Data source should be fully configured and ready-to-use.
     *
     * @param dataSrc Data source.
     * @return {@code This} for chaining.
     * @see CacheJdbcPojoStore#setDataSource(DataSource)
     */
    public CacheJdbcPojoStoreFactory<K, V> setDataSource(DataSource dataSrc) {
        this.dataSrc = dataSrc;

        return this;
    }

    /**
     * Set database dialect.
     *
     * @param dialect Database dialect.
     * @see CacheJdbcPojoStore#setDialect(JdbcDialect)
     */
    public void setDialect(JdbcDialect dialect) {
        this.dialect = dialect;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheJdbcPojoStoreFactory.class, this);
    }
}