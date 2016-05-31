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

package org.apache.ignite;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Grid Spring bean allows to bypass {@link Ignition} methods.
 * In other words, this bean class allows to inject new grid instance from
 * Spring configuration file directly without invoking static
 * {@link Ignition} methods. This class can be wired directly from
 * Spring and can be referenced from within other Spring beans.
 * By virtue of implementing {@link DisposableBean} and {@link InitializingBean}
 * interfaces, {@code GridSpringBean} automatically starts and stops underlying
 * grid instance.
 * <p>
 * <h1 class="header">Spring Configuration Example</h1>
 * Here is a typical example of describing it in Spring file:
 * <pre name="code" class="xml">
 * &lt;bean id="mySpringBean" class="org.apache.ignite.GridSpringBean"&gt;
 *     &lt;property name="configuration"&gt;
 *         &lt;bean id="grid.cfg" class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
 *             &lt;property name="gridName" value="mySpringGrid"/&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 * Or use default configuration:
 * <pre name="code" class="xml">
 * &lt;bean id="mySpringBean" class="org.apache.ignite.GridSpringBean"/&gt;
 * </pre>
 * <h1 class="header">Java Example</h1>
 * Here is how you may access this bean from code:
 * <pre name="code" class="java">
 * AbstractApplicationContext ctx = new FileSystemXmlApplicationContext("/path/to/spring/file");
 *
 * // Register Spring hook to destroy bean automatically.
 * ctx.registerShutdownHook();
 *
 * Grid grid = (Grid)ctx.getBean("mySpringBean");
 * </pre>
 * <p>
 */
public class IgniteSpringBean implements Ignite, DisposableBean, InitializingBean,
    ApplicationContextAware, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Ignite g;

    /** */
    private IgniteConfiguration cfg;

    /** */
    private ApplicationContext appCtx;

    /** {@inheritDoc} */
    @Override public IgniteConfiguration configuration() {
        return cfg;
    }

    /**
     * Gets the configuration of this Ignite instance.
     * <p>
     * This method is required for proper Spring integration and is the same as
     * {@link #configuration()}.
     * See https://issues.apache.org/jira/browse/IGNITE-1102 for details.
     * <p>
     * <b>NOTE:</b>
     * <br>
     * SPIs obtains through this method should never be used directly. SPIs provide
     * internal view on the subsystem and is used internally by Ignite kernal. In rare use cases when
     * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
     * via this method to check its configuration properties or call other non-SPI
     * methods.
     *
     * @return Ignite configuration instance.
     * @see #configuration()
     */
    public IgniteConfiguration getConfiguration() {
        return cfg;
    }

    /**
     * Sets Ignite configuration.
     *
     * @param cfg Ignite configuration.
     */
    public void setConfiguration(IgniteConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Gets the spring application context this Ignite runs in.
     *
     * @return Application context this Ignite runs in.
     */
    public ApplicationContext getApplicationContext() throws BeansException {
        return appCtx;
    }

    /** {@inheritDoc} */
    @Override public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        appCtx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws Exception {
        // If there were some errors when afterPropertiesSet() was called.
        if (g != null) {
            // Do not cancel started tasks, wait for them.
            G.stop(g.name(), false);
        }
    }

    /** {@inheritDoc} */
    @Override public void afterPropertiesSet() throws Exception {
        if (cfg == null)
            cfg = new IgniteConfiguration();

        g = IgniteSpring.start(cfg, appCtx);
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log() {
        checkIgnite();

        return cfg.getGridLogger();
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        checkIgnite();

        return g.version();
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute() {
        checkIgnite();

        return g.compute();
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services() {
        checkIgnite();

        return g.services();
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message() {
        checkIgnite();

        return g.message();
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events() {
        checkIgnite();

        return g.events();
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        checkIgnite();

        return g.executorService();
    }

    /** {@inheritDoc} */
    @Override public IgniteCluster cluster() {
        checkIgnite();

        return g.cluster();
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute(ClusterGroup grp) {
        checkIgnite();

        return g.compute(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message(ClusterGroup prj) {
        checkIgnite();

        return g.message(prj);
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events(ClusterGroup grp) {
        checkIgnite();

        return g.events(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services(ClusterGroup grp) {
        checkIgnite();

        return g.services(grp);
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup grp) {
        checkIgnite();

        return g.executorService(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        checkIgnite();

        return g.scheduler();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        checkIgnite();

        return g.name();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> cache(@Nullable String name) {
        checkIgnite();

        return g.cache(name);
    }


    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() {
        checkIgnite();

        return g.cacheNames();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg) {
        checkIgnite();

        return g.createCache(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) {
        checkIgnite();

        return g.getOrCreateCache(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) {
        checkIgnite();

        return g.createCache(cacheCfg, nearCfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg, NearCacheConfiguration<K, V> nearCfg) {
        checkIgnite();

        return g.getOrCreateCache(cacheCfg, nearCfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createNearCache(String cacheName, NearCacheConfiguration<K, V> nearCfg) {
        checkIgnite();

        return g.createNearCache(cacheName, nearCfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateNearCache(@Nullable String cacheName, NearCacheConfiguration<K, V> nearCfg) {
        checkIgnite();

        return g.getOrCreateNearCache(cacheName, nearCfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) {
        checkIgnite();

        return g.getOrCreateCache(cacheName);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(String cacheName) {
        checkIgnite();

        return g.createCache(cacheName);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) {
        checkIgnite();

        g.addCacheConfiguration(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        checkIgnite();

        g.destroyCache(cacheName);
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        checkIgnite();

        return g.transactions();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String cacheName) {
        checkIgnite();

        return g.dataStreamer(cacheName);
    }

    /** {@inheritDoc} */
    @Override public IgniteFileSystem fileSystem(String name) {
        checkIgnite();

        return g.fileSystem(name);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFileSystem> fileSystems() {
        checkIgnite();

        return g.fileSystems();
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        checkIgnite();

        return g.plugin(name);
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        checkIgnite();

        return g.binary();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        g.close();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create) {
        checkIgnite();

        return g.atomicSequence(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) {
        checkIgnite();

        return g.atomicLong(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteAtomicReference<T> atomicReference(String name,
        @Nullable T initVal,
        boolean create)
    {
        checkIgnite();

        return g.atomicReference(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name,
        @Nullable T initVal,
        @Nullable S initStamp,
        boolean create)
    {
        checkIgnite();

        return g.atomicStamped(name, initVal, initStamp, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteCountDownLatch countDownLatch(String name,
        int cnt,
        boolean autoDel,
        boolean create)
    {
        checkIgnite();

        return g.countDownLatch(name, cnt, autoDel, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteSemaphore semaphore(String name,
        int cnt,
        boolean failoverSafe,
        boolean create)
    {
        checkIgnite();

        return g.semaphore(name, cnt,
            failoverSafe, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteLock reentrantLock(String name,
        boolean failoverSafe,
        boolean fair,
        boolean create)
    {
        checkIgnite();

        return g.reentrantLock(name, failoverSafe, create, fair);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteQueue<T> queue(String name,
        int cap,
        CollectionConfiguration cfg)
    {
        checkIgnite();

        return g.queue(name, cap, cfg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteSet<T> set(String name,
        CollectionConfiguration cfg)
    {
        checkIgnite();

        return g.set(name, cfg);
    }

    /** {@inheritDoc} */
    @Override public <K> Affinity<K> affinity(String cacheName) {
        return g.affinity(cacheName);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(g);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        g = (Ignite)in.readObject();

        cfg = g.configuration();
    }

    /**
     * Checks if this bean is valid.
     *
     * @throws IllegalStateException If bean is not valid, i.e. Ignite has already been stopped
     *      or has not yet been started.
     */
    protected void checkIgnite() throws IllegalStateException {
        if (g == null) {
            throw new IllegalStateException("Ignite is in invalid state to perform this operation. " +
                "It either not started yet or has already being or have stopped " +
                "[ignite=" + g + ", cfg=" + cfg + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteSpringBean.class, this);
    }
}
