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

package org.apache.ignite.cache.store.hibernate;

import java.util.Properties;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.hibernate.SessionFactory;

/**
 * {@link Factory} implementation for {@link CacheHibernateBlobStore}.
 *
 * Use this factory to pass {@link CacheHibernateBlobStore} to {@link CacheConfiguration}.
 *
 * <h2 class="header">Java Example</h2>
 * In this example existing session factory is provided.
 * <pre name="code" class="java">
 *     ...
 *     CacheHibernateBlobStoreFactory&lt;String, String&gt; factory = new CacheHibernateBlobStoreFactory&lt;String, String&gt;();
 *
 *     factory.setSessionFactory(sesFactory);
 *     ...
 * </pre>
 *
 * <h2 class="header">Spring Example (using Spring ORM)</h2>
 * <pre name="code" class="xml">
 *   ...
 *   &lt;bean id=&quot;cache.hibernate.store.factory&quot;
 *       class=&quot;org.apache.ignite.cache.store.hibernate.CacheHibernateBlobStoreFactory&quot;&gt;
 *       &lt;property name=&quot;sessionFactory&quot;&gt;
 *           &lt;bean class=&quot;org.springframework.orm.hibernate3.LocalSessionFactoryBean&quot;&gt;
 *               &lt;property name=&quot;hibernateProperties&quot;&gt;
 *                   &lt;value&gt;
 *                       connection.url=jdbc:h2:mem:
 *                       show_sql=true
 *                       hbm2ddl.auto=true
 *                       hibernate.dialect=org.hibernate.dialect.H2Dialect
 *                   &lt;/value&gt;
 *               &lt;/property&gt;
 *               &lt;property name=&quot;mappingResources&quot;&gt;
 *                   &lt;list&gt;
 *                       &lt;value&gt;
 *                           org/apache/ignite/cache/store/hibernate/CacheHibernateBlobStoreEntry.hbm.xml
 *                       &lt;/value&gt;
 *                   &lt;/list&gt;
 *               &lt;/property&gt;
 *           &lt;/bean&gt;
 *       &lt;/property&gt;
 *   &lt;/bean&gt;
 *   ...
 * </pre>
 *
 * <h2 class="header">Spring Example (using Spring ORM and persistent annotations)</h2>
 * <pre name="code" class="xml">
 *     ...
 *     &lt;bean id=&quot;cache.hibernate.store.factory1&quot;
 *         class=&quot;org.apache.ignite.cache.store.hibernate.CacheHibernateBlobStoreFactory&quot;&gt;
 *         &lt;property name=&quot;sessionFactory&quot;&gt;
 *             &lt;bean class=&quot;org.springframework.orm.hibernate3.annotation.AnnotationSessionFactoryBean&quot;&gt;
 *                 &lt;property name=&quot;hibernateProperties&quot;&gt;
 *                     &lt;value&gt;
 *                         connection.url=jdbc:h2:mem:
 *                         show_sql=true
 *                         hbm2ddl.auto=true
 *                         hibernate.dialect=org.hibernate.dialect.H2Dialect
 *                     &lt;/value&gt;
 *                 &lt;/property&gt;
 *                 &lt;property name=&quot;annotatedClasses&quot;&gt;
 *                     &lt;list&gt;
 *                         &lt;value&gt;
 *                             org.apache.ignite.cache.store.hibernate.CacheHibernateBlobStoreEntry
 *                         &lt;/value&gt;
 *                     &lt;/list&gt;
 *                 &lt;/property&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 *     ...
 * </pre>
 *
 * <h2 class="header">Spring Example</h2>
 * <pre name="code" class="xml">
 *     ...
 *     &lt;bean id=&quot;cache.hibernate.store.factory2&quot;
 *         class=&quot;org.apache.ignite.cache.store.hibernate.CacheHibernateBlobStoreFactory&quot;&gt;
 *         &lt;property name=&quot;hibernateProperties&quot;&gt;
 *             &lt;props&gt;
 *                 &lt;prop key=&quot;connection.url&quot;&gt;jdbc:h2:mem:&lt;/prop&gt;
 *                 &lt;prop key=&quot;hbm2ddl.auto&quot;&gt;update&lt;/prop&gt;
 *                 &lt;prop key=&quot;show_sql&quot;&gt;true&lt;/prop&gt;
 *             &lt;/props&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 *     ...
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
public class CacheHibernateBlobStoreFactory<K, V> implements Factory<CacheHibernateBlobStore<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Session factory. */
    @GridToStringExclude
    private transient SessionFactory sesFactory;

    /** Session factory bean name. */
    private String sesFactoryBean;

    /** Path to hibernate configuration file. */
    private String hibernateCfgPath;

    /** Hibernate properties. */
    @GridToStringExclude
    private Properties hibernateProps;

    /** Application context. */
    @SpringApplicationContextResource
    private Object appCtx;

    /** {@inheritDoc} */
    @Override public CacheHibernateBlobStore<K, V> create() {
        CacheHibernateBlobStore<K, V> store = new CacheHibernateBlobStore<>();

        store.setHibernateConfigurationPath(hibernateCfgPath);
        store.setHibernateProperties(hibernateProps);

        if (sesFactory != null)
            store.setSessionFactory(sesFactory);
        else if (sesFactoryBean != null) {
            if (appCtx == null)
                throw new IgniteException("Spring application context resource is not injected.");

            IgniteSpringHelper spring;

            try {
                spring = IgniteComponentType.SPRING.create(false);

                SessionFactory sesFac = spring.loadBeanFromAppContext(appCtx, sesFactoryBean);

                store.setSessionFactory(sesFac);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to load bean in application context [beanName=" + sesFactoryBean +
                        ", igniteConfig=" + appCtx + ']');
            }
        }

        return store;
    }

    /**
     * Sets session factory.
     *
     * @param sesFactory Session factory.
     * @return {@code This} for chaining.
     * @see CacheHibernateBlobStore#setSessionFactory(SessionFactory)
     */
    public CacheHibernateBlobStoreFactory<K, V> setSessionFactory(SessionFactory sesFactory) {
        this.sesFactory = sesFactory;

        return this;
    }

    /**
     * Sets name of the data source bean.
     *
     * @param sesFactory Session factory bean name.
     * @return {@code This} for chaining.
     * @see CacheHibernateBlobStore#setSessionFactory(SessionFactory)
     */
    public CacheHibernateBlobStoreFactory<K, V> setSessionFactoryBean(String sesFactory) {
        this.sesFactoryBean = sesFactory;

        return this;
    }

    /**
     * Sets hibernate configuration path.
     * <p>
     * This may be either URL or file path or classpath resource.
     *
     * @param hibernateCfgPath URL or file path or classpath resource
     *      pointing to hibernate configuration XML file.
     * @return {@code This} for chaining.
     * @see CacheHibernateBlobStore#setHibernateConfigurationPath(String)
     */
    public CacheHibernateBlobStoreFactory<K, V> setHibernateConfigurationPath(String hibernateCfgPath) {
        this.hibernateCfgPath = hibernateCfgPath;

        return this;
    }

    /**
     * Sets Hibernate properties.
     *
     * @param hibernateProps Hibernate properties.
     * @return {@code This} for chaining.
     * @see CacheHibernateBlobStore#setHibernateProperties(Properties)
     */
    public CacheHibernateBlobStoreFactory<K, V> setHibernateProperties(Properties hibernateProps) {
        this.hibernateProps = hibernateProps;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheHibernateBlobStoreFactory.class, this);
    }
}
