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

package org.gridgain.grid;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.plugin.*;
import org.apache.ignite.product.*;
import org.apache.ignite.hadoop.*;
import org.apache.ignite.plugin.security.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
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
 * &lt;bean id="mySpringBean" class="org.gridgain.grid.GridSpringBean"&gt;
 *     &lt;property name="configuration"&gt;
 *         &lt;bean id="grid.cfg" class="org.gridgain.grid.GridConfiguration"&gt;
 *             &lt;property name="gridName" value="mySpringGrid"/&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 * Or use default configuration:
 * <pre name="code" class="xml">
 * &lt;bean id="mySpringBean" class="org.gridgain.grid.GridSpringBean"/&gt;
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
public class GridSpringBean extends GridMetadataAwareAdapter implements Ignite, DisposableBean, InitializingBean,
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

        g = GridGainSpring.start(cfg, appCtx);
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log() {
        assert cfg != null;

        return cfg.getGridLogger();
    }

    /** {@inheritDoc} */
    @Override public IgniteProduct product() {
        assert g != null;

        return g.product();
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
    @Override public IgniteManaged managed() {
        assert g != null;

        return g.managed();
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
    @Override public IgniteCompute compute(ClusterGroup prj) {
        assert g != null;

        return g.compute(prj);
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message(ClusterGroup prj) {
        assert g != null;

        return g.message(prj);
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events(ClusterGroup prj) {
        assert g != null;

        return g.events(prj);
    }

    /** {@inheritDoc} */
    @Override public IgniteManaged managed(ClusterGroup prj) {
        assert g != null;

        return g.managed(prj);
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup prj) {
        assert g != null;

        return g.executorService(prj);
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        assert g != null;

        return g.scheduler();
    }

    /** {@inheritDoc} */
    @Override public GridSecurity security() {
        assert g != null;

        return g.security();
    }

    /** {@inheritDoc} */
    @Override public IgnitePortables portables() {
        assert g != null;

        return g.portables();
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
    @Override public GridHadoop hadoop() {
        assert g != null;

        return g.hadoop();
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
    @Override public void close() throws IgniteCheckedException {
        g.close();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSpringBean.class, this);
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
