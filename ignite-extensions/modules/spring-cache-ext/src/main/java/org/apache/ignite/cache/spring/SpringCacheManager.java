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

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteSpring;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.springdata.proxy.IgniteNodeCacheProxy;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * Implementation of Spring cache abstraction based on Ignite cache.
 * <h1 class="header">Overview</h1>
 * Spring cache abstraction allows to enable caching for Java methods
 * so that the result of a method execution is stored in some storage. If
 * later the same method is called with the same set of parameters,
 * the result will be retrieved from that storage instead of actually
 * executing the method. For more information, refer to
 * <a href="http://docs.spring.io/spring/docs/current/spring-framework-reference/html/cache.html">
 * Spring Cache Abstraction documentation</a>.
 * <h1 class="header">How To Enable Caching</h1>
 * To enable caching based on Ignite cache in your Spring application,
 * you will need to do the following:
 * <ul>
 *     <li>
 *         Start an Ignite node with proper configuration in embedded mode
 *         (i.e., in the same JVM where the application is running). It can
 *         already have predefined caches, but it's not required - caches
 *         will be created automatically on first access if needed.
 *     </li>
 *     <li>
 *         Configure {@code SpringCacheManager} as a cache provider
 *         in the Spring application context.
 *     </li>
 * </ul>
 * {@code SpringCacheManager} can start a node itself on its startup
 * based on provided Ignite configuration. You can provide path to a
 * Spring configuration XML file, like below (path can be absolute or
 * relative to {@code IGNITE_HOME}):
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:cache="http://www.springframework.org/schema/cache"
 *        xsi:schemaLocation="
 *         http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *         http://www.springframework.org/schema/cache http://www.springframework.org/schema/cache/spring-cache.xsd"&gt;
 *     &lt;-- Provide configuration file path. --&gt;
 *     &lt;bean id="cacheManager" class="org.apache.ignite.cache.spring.SpringCacheManager"&gt;
 *         &lt;property name="configurationPath" value="examples/config/spring-cache.xml"/&gt;
 *     &lt;/bean&gt;
 *
 *     &lt;-- Use annotation-driven caching configuration. --&gt;
 *     &lt;cache:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 * Or you can provide a {@link IgniteConfiguration} bean, like below:
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:cache="http://www.springframework.org/schema/cache"
 *        xsi:schemaLocation="
 *         http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *         http://www.springframework.org/schema/cache http://www.springframework.org/schema/cache/spring-cache.xsd"&gt;
 *     &lt;-- Provide configuration bean. --&gt;
 *     &lt;bean id="cacheManager" class="org.apache.ignite.cache.spring.SpringCacheManager"&gt;
 *         &lt;property name="configuration"&gt;
 *             &lt;bean id="gridCfg" class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
 *                 ...
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 *
 *     &lt;-- Use annotation-driven caching configuration. --&gt;
 *     &lt;cache:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 * Note that providing both configuration path and configuration bean is illegal
 * and results in {@link IllegalArgumentException}.
 * <p>
 * If you already have Ignite node running within your application,
 * simply provide correct Ignite instance name, like below (if there is no Grid
 * instance with such name, exception will be thrown):
 * <pre name="code" class="xml">
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:cache="http://www.springframework.org/schema/cache"
 *        xsi:schemaLocation="
 *         http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *         http://www.springframework.org/schema/cache http://www.springframework.org/schema/cache/spring-cache.xsd"&gt;
 *     &lt;-- Provide Ignite instance name. --&gt;
 *     &lt;bean id="cacheManager" class="org.apache.ignite.cache.spring.SpringCacheManager"&gt;
 *         &lt;property name="igniteInstanceName" value="myGrid"/&gt;
 *     &lt;/bean>
 *
 *     &lt;-- Use annotation-driven caching configuration. --&gt;
 *     &lt;cache:annotation-driven/&gt;
 * &lt;/beans&gt;
 * </pre>
 * This can be used, for example, when you are running your application
 * in a J2EE Web container and use {@ignitelink org.apache.ignite.startup.servlet.ServletContextListenerStartup}
 * for node startup.
 * <p>
 * If neither {@link #setConfigurationPath(String) configurationPath},
 * {@link #setConfiguration(IgniteConfiguration) configuration}, nor
 * {@link #setIgniteInstanceName(String) igniteInstanceName} are provided, cache manager
 * will try to use default Grid instance (the one with the {@code null}
 * name). If it doesn't exist, exception will be thrown.
 * <h1>Starting Remote Nodes</h1>
 * Keep in mind that the node started inside your application is an entry point
 * to the whole topology it connects to. You can start as many remote standalone
 * nodes as you need using {@code bin/ignite.{sh|bat}} scripts provided in
 * Ignite distribution, and all these nodes will participate
 * in caching the data.
 */
public class SpringCacheManager extends AbstractCacheManager implements ApplicationListener<ContextRefreshedEvent>,
    ApplicationContextAware, DisposableBean {
    /** Default locks count. */
    private static final int DEFAULT_LOCKS_COUNT = 512;

    /** IgniteLock name prefix. */
    private static final String SPRING_LOCK_NAME_PREFIX = "springSync";

    /** Grid configuration file path. */
    private String cfgPath;

    /** Ignite configuration. */
    private IgniteConfiguration cfg;

    /** Ignite instance name. */
    private String igniteInstanceName;

    /** Count of IgniteLocks are used for sync get */
    private int locksCnt = DEFAULT_LOCKS_COUNT;

    /** Dynamic cache configuration template. */
    private CacheConfiguration<Object, Object> dynamicCacheCfg;

    /** Dynamic near cache configuration template. */
    private NearCacheConfiguration<Object, Object> dynamicNearCacheCfg;

    /** Ignite instance. */
    private Ignite ignite;

    /** Spring context. */
    private ApplicationContext springCtx;

    /** Locks for value loading to support sync option. */
    private ConcurrentHashMap<Integer, IgniteLock> locks = new ConcurrentHashMap<>();

    /** Flag that indicates whether external Ignite instance is configured. */
    private boolean externalIgniteInstance;

    /** {@inheritDoc} */
    @Override public void setApplicationContext(ApplicationContext ctx) {
        this.springCtx = ctx;
    }

    /**
     * Gets configuration file path.
     *
     * @return Grid configuration file path.
     */
    public String getConfigurationPath() {
        return cfgPath;
    }

    /**
     * Sets configuration file path.
     *
     * @param cfgPath Grid configuration file path.
     */
    public void setConfigurationPath(String cfgPath) {
        this.cfgPath = cfgPath;
    }

    /**
     * Gets configuration bean.
     *
     * @return Grid configuration bean.
     */
    public IgniteConfiguration getConfiguration() {
        return cfg;
    }

    /**
     * Sets configuration bean.
     *
     * @param cfg Grid configuration bean.
     */
    public void setConfiguration(IgniteConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Gets grid name.
     *
     * @return Grid name.
     * @deprecated Use {@link #getIgniteInstanceName()}.
     */
    @Deprecated
    public String getGridName() {
        return getIgniteInstanceName();
    }

    /**
     * Sets grid name.
     *
     * @param gridName Grid name.
     * @deprecated Use {@link #setIgniteInstanceName(String)}.
     */
    @Deprecated
    public void setGridName(String gridName) {
        setIgniteInstanceName(gridName);
    }

    /**
     * Gets Ignite instance name.
     *
     * @return Ignite instance name.
     */
    public String getIgniteInstanceName() {
        return igniteInstanceName;
    }

    /**
     * Sets Ignite instance name.
     *
     * @param igniteInstanceName Ignite instance name.
     */
    public void setIgniteInstanceName(String igniteInstanceName) {
        this.igniteInstanceName = igniteInstanceName;
    }

    /**
     * Gets locks count.
     *
     * @return locks count.
     */
    public int getLocksCount() {
        return locksCnt;
    }

    /**
     * @param locksCnt locks count.
     */
    public void setLocksCount(int locksCnt) {
        this.locksCnt = locksCnt;
    }

    /**
     * Gets dynamic cache configuration template.
     *
     * @return Dynamic cache configuration template.
     */
    public CacheConfiguration<Object, Object> getDynamicCacheConfiguration() {
        return dynamicCacheCfg;
    }

    /**
     * Sets dynamic cache configuration template.
     *
     * @param dynamicCacheCfg Dynamic cache configuration template.
     */
    public void setDynamicCacheConfiguration(CacheConfiguration<Object, Object> dynamicCacheCfg) {
        this.dynamicCacheCfg = dynamicCacheCfg;
    }

    /**
     * Gets dynamic near cache configuration template.
     *
     * @return Dynamic near cache configuration template.
     */
    public NearCacheConfiguration<Object, Object> getDynamicNearCacheConfiguration() {
        return dynamicNearCacheCfg;
    }

    /**
     * Sets dynamic cache configuration template.
     *
     * @param dynamicNearCacheCfg Dynamic cache configuration template.
     */
    public void setDynamicNearCacheConfiguration(NearCacheConfiguration<Object, Object> dynamicNearCacheCfg) {
        this.dynamicNearCacheCfg = dynamicNearCacheCfg;
    }

    /** {@inheritDoc} */
    @Override public void onApplicationEvent(ContextRefreshedEvent event) {
        if (ignite == null) {

            if (cfgPath != null && cfg != null) {
                throw new IllegalArgumentException("Both 'configurationPath' and 'configuration' are " +
                    "provided. Set only one of these properties if you need to start a Ignite node inside of " +
                    "SpringCacheManager. If you already have a node running, omit both of them and set" +
                    "'igniteInstanceName' property.");
            }

            try {
                if (cfgPath != null) {
                    ignite = IgniteSpring.start(cfgPath, springCtx);
                }
                else if (cfg != null)
                    ignite = IgniteSpring.start(cfg, springCtx);
                else {
                    ignite = Ignition.ignite(igniteInstanceName);

                    externalIgniteInstance = true;
                }
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected SpringCache createCache(String name) {
        CacheConfiguration<Object, Object> cacheCfg = dynamicCacheCfg != null ?
            new CacheConfiguration<>(dynamicCacheCfg) : new CacheConfiguration<>();

        NearCacheConfiguration<Object, Object> nearCacheCfg = dynamicNearCacheCfg != null ?
            new NearCacheConfiguration<>(dynamicNearCacheCfg) : null;

        cacheCfg.setName(name);

        IgniteCache<Object, Object> cache = nearCacheCfg != null
            ? ignite.getOrCreateCache(cacheCfg, nearCacheCfg)
            : ignite.getOrCreateCache(cacheCfg);

        return new SpringCache(new IgniteNodeCacheProxy<>(cache), this);
    }

    /** {@inheritDoc} */
    @Override protected IgniteLock getSyncLock(String name, Object key) {
        int hash = Objects.hash(name, key);

        final int idx = hash % getLocksCount();

        return locks.computeIfAbsent(idx, i -> ignite.reentrantLock(SPRING_LOCK_NAME_PREFIX + idx, true, false, true));
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        if (!externalIgniteInstance)
            ignite.close();
    }
}
