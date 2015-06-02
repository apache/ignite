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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.resources.*;
import org.springframework.context.*;

import javax.cache.configuration.*;
import javax.sql.*;

/**
 * {@link Factory} implementation for {@link CacheJdbcBlobStore}.
 *
 * Use this factory to pass {@link CacheJdbcBlobStore} to {@link org.apache.ignite.configuration.CacheConfiguration}.
 *
 * <h2 class="header">Spring Example</h2>
 * <pre name="code" class="xml"> *
 *     &lt;bean id= "simpleDataSource" class="org.h2.jdbcx.JdbcDataSource"/&gt;
 *
 *     &lt;bean id=&quot;cache.jdbc.store&quot;
 *         class=&quot;org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore&quot;&gt;
 *         &lt;property name=&quot;connectionUrl&quot; value=&quot;jdbc:h2:mem:&quot;/&gt;
 *         &lt;property name=&quot;createTableQuery&quot;
 *             value=&quot;create table if not exists ENTRIES (key other, val other)&quot;/&gt;
 *     &lt;/bean&gt;
 *
 *     &lt;bean id="ignite.cfg" class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
 *          ...
 *          &lt;property name="cacheConfiguration"&gt;
 *               &lt;list&gt;
 *                  &lt;bean class="org.apache.ignite.configuration.CacheConfiguration"&gt;
 *                      ...
 *                      &lt;property name="cacheStoreFactory"&gt;
 *                          &lt;bean class="org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStoreFactory"&gt;
 *                              &lt;property name="user" value = "GridGain" /&gt;
 *                              &lt;property name="dataSourceBean" value = "simpleDataSource" /&gt;
 *                          &lt;/bean&gt;
 *                      &lt;/property&gt;
 *                  &lt;/bean&gt;
 *               &lt;/list&gt;
 *          &lt;/property&gt;
 *     &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.incubator.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
public class CacheJdbcBlobStoreFactory<K, V>  implements Factory<CacheJdbcBlobStore<K, V>> {
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

    /** Application context. */
    @SpringApplicationContextResource
    private ApplicationContext appContext;

    /** {@inheritDoc} */
    @Override public CacheJdbcBlobStore<K, V> create() {
        CacheJdbcBlobStore<K, V> store = new CacheJdbcBlobStore();

        store.setInitSchema(initSchema);
        store.setConnectionUrl(connUrl);
        store.setCreateTableQuery(createTblQry);
        store.setLoadQuery(loadQry);
        store.setUpdateQuery(updateQry);
        store.setInsertQuery(insertQry);
        store.setDeleteQuery(delQry);
        store.setUser(user);
        store.setPassword(passwd);

        if (dataSrcBean != null) {
            if (appContext == null)
                throw new IgniteException("Spring application context resource is not injected.");

            if (!appContext.containsBean(dataSrcBean))
                throw new IgniteException("Cannot find bean in application context. [beanName=" + dataSrcBean + "].");

            DataSource data = (DataSource) appContext.getBean(dataSrcBean);

            store.setDataSource(data);
        }

        return store;
    }

    /**
     * See {@link org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore#setInitSchema(boolean)}.
     *
     * @param initSchema Initialized schema flag.
     */
    public void setInitSchema(boolean initSchema) {
        this.initSchema = initSchema;
    }

    /**
     * See {@link org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore#setConnectionUrl(String)}.
     *
     * @param connUrl Connection URL.
     */
    public void setConnectionUrl(String connUrl) {
        this.connUrl = connUrl;
    }

    /**
     * See {@link org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore#setCreateTableQuery(String)}.
     *
     * @param createTblQry Create table query.
     */
    public void setCreateTableQuery(String createTblQry) {
        this.createTblQry = createTblQry;
    }

    /**
     * See {@link org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore#setLoadQuery(String)}.
     *
     * @param loadQry Load query
     */
    public void setLoadQuery(String loadQry) {
        this.loadQry = loadQry;
    }

    /**
     * See {@link org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore#setUpdateQuery(String)}.
     *
     * @param updateQry Update entry query.
     */
    public void setUpdateQuery(String updateQry) {
        this.updateQry = updateQry;
    }

    /**
     * See {@link org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore#setInsertQuery(String)}.
     *
     * @param insertQry Insert entry query.
     */
    public void setInsertQuery(String insertQry) {
        this.insertQry = insertQry;
    }

    /**
     * See {@link org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore#setDeleteQuery(String)}.
     *
     * @param delQry Delete entry query.
     */
    public void setDeleteQuery(String delQry) {
        this.delQry = delQry;
    }

    /**
     * See {@link org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore#setUser(String)}.
     *
     * @param user User name.
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * See {@link org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore#setPassword(String)}.
     *
     * @param passwd Password.
     */
    public void setPassword(String passwd) {
        this.passwd = passwd;
    }

    /**
     * Sets name of the data source bean.
     *
     * See {@link org.apache.ignite.cache.store.jdbc.CacheJdbcBlobStore#setDataSource(javax.sql.DataSource)}
     * for more information.
     *
     * @param dataSrcBean Data source bean name.
     */
    public void setDataSourceBean(String dataSrcBean) {
        this.dataSrcBean = dataSrcBean;
    }
}
