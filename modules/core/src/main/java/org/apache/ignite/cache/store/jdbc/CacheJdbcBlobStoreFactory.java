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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.SpringApplicationContextResource;

/**
 * {@link Factory} implementation for {@link CacheJdbcBlobStore}.
 *
 * Use this factory to pass {@link CacheJdbcBlobStore} to {@link CacheConfiguration}.
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
 *                      ...
 *                      &lt;property name="cacheStoreFactory"&gt;
 *                          &lt;bean class="org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStoreFactory"&gt;
 *                              &lt;property name="user" value = "Ignite" /&gt;
 *                              &lt;property name="dataSourceBean" value = "myDataSource" /&gt;
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
public class CacheJdbcBlobStoreFactory<K, V> implements Factory<CacheJdbcBlobStore<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Connection URL. */
    private String connUrl = CacheJdbcBlobStore.DFLT_CONN_URL;

    /** Query to create table. */
    private String createTblQry = CacheJdbcBlobStore.DFLT_CREATE_TBL_QRY;

    /** Query to load entry. */
    private String loadQry = CacheJdbcBlobStore.DFLT_LOAD_QRY;

    /** Query to update entry. */
    private String updateQry = CacheJdbcBlobStore.DFLT_UPDATE_QRY;

    /** Query to insert entries. */
    private String insertQry = CacheJdbcBlobStore.DFLT_INSERT_QRY;

    /** Query to delete entries. */
    private String delQry = CacheJdbcBlobStore.DFLT_DEL_QRY;

    /** User name for database access. */
    private String user;

    /** Password for database access. */
    @GridToStringExclude
    private String passwd;

    /** Flag for schema initialization. */
    private boolean initSchema = true;

    /** Name of data source bean. */
    private String dataSrcBean;

    /** Data source. */
    private transient DataSource dataSrc;

    /** Application context. */
    @SpringApplicationContextResource
    private Object appCtx;

    /** {@inheritDoc} */
    @Override public CacheJdbcBlobStore<K, V> create() {
        CacheJdbcBlobStore<K, V> store = new CacheJdbcBlobStore<>();

        store.setInitSchema(initSchema);
        store.setConnectionUrl(connUrl);
        store.setCreateTableQuery(createTblQry);
        store.setLoadQuery(loadQry);
        store.setUpdateQuery(updateQry);
        store.setInsertQuery(insertQry);
        store.setDeleteQuery(delQry);
        store.setUser(user);
        store.setPassword(passwd);

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
            catch (IgniteCheckedException ignored) {
                throw new IgniteException("Failed to load bean in application context [beanName=" + dataSrcBean +
                    ", igniteConfig=" + appCtx + ']');
            }
        }

        return store;
    }

    /**
     * Flag indicating whether DB schema should be initialized by Ignite (default behaviour) or
     * was explicitly created by user.
     *
     * @param initSchema Initialized schema flag.
     * @return {@code This} for chaining.
     * @see CacheJdbcBlobStore#setInitSchema(boolean)
     */
    public CacheJdbcBlobStoreFactory<K, V> setInitSchema(boolean initSchema) {
        this.initSchema = initSchema;

        return this;
    }

    /**
     * Sets connection URL.
     *
     * @param connUrl Connection URL.
     * @return {@code This} for chaining.
     * @see CacheJdbcBlobStore#setConnectionUrl(String)
     */
    public CacheJdbcBlobStoreFactory<K, V> setConnectionUrl(String connUrl) {
        this.connUrl = connUrl;

        return this;
    }

    /**
     * Sets create table query.
     *
     * @param createTblQry Create table query.
     * @return {@code This} for chaining.
     * @see CacheJdbcBlobStore#setCreateTableQuery(String)
     */
    public CacheJdbcBlobStoreFactory<K, V> setCreateTableQuery(String createTblQry) {
        this.createTblQry = createTblQry;

        return this;
    }

    /**
     * Sets load query.
     *
     * @param loadQry Load query
     * @return {@code This} for chaining.
     * @see CacheJdbcBlobStore#setLoadQuery(String)
     */
    public CacheJdbcBlobStoreFactory<K, V> setLoadQuery(String loadQry) {
        this.loadQry = loadQry;

        return this;
    }

    /**
     * Sets update entry query.
     *
     * @param updateQry Update entry query.
     * @return {@code This} for chaining.
     * @see  CacheJdbcBlobStore#setUpdateQuery(String)
     */
    public CacheJdbcBlobStoreFactory<K, V> setUpdateQuery(String updateQry) {
        this.updateQry = updateQry;

        return this;
    }

    /**
     * Sets insert entry query.
     *
     * @param insertQry Insert entry query.
     * @return {@code This} for chaining.
     * @see CacheJdbcBlobStore#setInsertQuery(String)
     */
    public CacheJdbcBlobStoreFactory<K, V> setInsertQuery(String insertQry) {
        this.insertQry = insertQry;

        return this;
    }

    /**
     * Sets delete entry query.
     *
     * @param delQry Delete entry query.
     * @return {@code This} for chaining.
     * @see CacheJdbcBlobStore#setDeleteQuery(String)
     */
    public CacheJdbcBlobStoreFactory<K, V> setDeleteQuery(String delQry) {
        this.delQry = delQry;

        return this;
    }

    /**
     * Sets user name for database access.
     *
     * @param user User name.
     * @return {@code This} for chaining.
     * @see CacheJdbcBlobStore#setUser(String)
     */
    public CacheJdbcBlobStoreFactory<K, V> setUser(String user) {
        this.user = user;

        return this;
    }

    /**
     * Sets password for database access.
     *
     * @param passwd Password.
     * @return {@code This} for chaining.
     * @see CacheJdbcBlobStore#setPassword(String)
     */
    public CacheJdbcBlobStoreFactory<K, V> setPassword(String passwd) {
        this.passwd = passwd;

        return this;
    }

    /**
     * Sets name of the data source bean.
     *
     * @param dataSrcBean Data source bean name.
     * @return {@code This} for chaining.
     * @see CacheJdbcBlobStore#setDataSource(DataSource)
     */
    public CacheJdbcBlobStoreFactory<K, V> setDataSourceBean(String dataSrcBean) {
        this.dataSrcBean = dataSrcBean;

        return this;
    }

    /**
     * Sets data source. Data source should be fully configured and ready-to-use.
     *
     * @param dataSrc Data source.
     * @return {@code This} for chaining.
     * @see CacheJdbcBlobStore#setDataSource(DataSource)
     */
    public CacheJdbcBlobStoreFactory<K, V> setDataSource(DataSource dataSrc) {
        this.dataSrc = dataSrc;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheJdbcBlobStoreFactory.class, this);
    }
}
