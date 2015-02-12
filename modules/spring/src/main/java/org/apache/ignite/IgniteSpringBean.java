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

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import org.springframework.beans.*;
import org.springframework.beans.factory.*;
import org.springframework.context.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

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
     * Sets grid configuration.
     *
     * @param cfg Grid configuration.
     */
    public void setConfiguration(IgniteConfiguration cfg) {
        this.cfg = cfg;
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
        assert cfg != null;

        return cfg.getGridLogger();
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        assert g != null;

        return g.version();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCache<?, ?>> caches() {
        assert g != null;

        return g.caches();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteStreamer> streamers() {
        assert g != null;

        return g.streamers();
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute() {
        assert g != null;

        return g.compute();
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services() {
        assert g != null;

        return g.services();
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message() {
        assert g != null;

        return g.message();
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events() {
        assert g != null;

        return g.events();
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        assert g != null;

        return g.executorService();
    }

    /** {@inheritDoc} */
    @Override public IgniteCluster cluster() {
        assert g != null;

        return g.cluster();
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute(ClusterGroup grp) {
        assert g != null;

        return g.compute(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message(ClusterGroup prj) {
        assert g != null;

        return g.message(prj);
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events(ClusterGroup grp) {
        assert g != null;

        return g.events(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services(ClusterGroup grp) {
        assert g != null;

        return g.services(grp);
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup grp) {
        assert g != null;

        return g.executorService(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        assert g != null;

        return g.scheduler();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        assert g != null;

        return g.name();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCache<K, V> cache(String name) {
        assert g != null;

        return g.cache(name);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> jcache(@Nullable String name) {
        assert g != null;

        return g.jcache(name);
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        assert g != null;

        return g.transactions();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataLoader<K, V> dataLoader(@Nullable String cacheName) {
        assert g != null;

        return g.dataLoader(cacheName);
    }

    /** {@inheritDoc} */
    @Override public IgniteFs fileSystem(String name) {
        assert g != null;

        return g.fileSystem(name);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFs> fileSystems() {
        assert g != null;

        return g.fileSystems();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteStreamer streamer(@Nullable String name) {
        assert g != null;

        return g.streamer(name);
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        assert g != null;

        return g.plugin(name);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        g.close();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create) {
        assert g != null;

        return g.atomicSequence(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) {
        assert g != null;

        return g.atomicLong(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteAtomicReference<T> atomicReference(String name,
        @Nullable T initVal,
        boolean create)
    {
        assert g != null;

        return g.atomicReference(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name,
        @Nullable T initVal,
        @Nullable S initStamp,
        boolean create)
    {
        assert g != null;

        return g.atomicStamped(name, initVal, initStamp, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteCountDownLatch countDownLatch(String name,
        int cnt,
        boolean autoDel,
        boolean create)
    {
        assert g != null;

        return g.countDownLatch(name, cnt, autoDel, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteQueue<T> queue(String name,
        int cap,
        CollectionConfiguration cfg)
    {
        assert g != null;

        return g.queue(name, cap, cfg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteSet<T> set(String name,
        CollectionConfiguration cfg)
    {
        assert g != null;

        return g.set(name, cfg);
    }

    /** {@inheritDoc} */
    @Override public <K> CacheAffinity<K> affinity(String cacheName) {
        return g.affinity(cacheName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteSpringBean.class, this);
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
}
