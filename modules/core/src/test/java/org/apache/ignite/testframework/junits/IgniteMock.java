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

package org.apache.ignite.testframework.junits;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import javax.management.MBeanServer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite mock.
 */
public class IgniteMock implements Ignite {
    /** Ignite name */
    private final String name;

    /** Local host. */
    private final String locHost;

    /** */
    private final UUID nodeId;

    /** */
    private Marshaller marshaller;

    /** */
    private final MBeanServer jmx;

    /** */
    private final String home;

    /** */
    private IgniteConfiguration staticCfg;

    /**
     * Mock values
     *
     * @param name Name.
     * @param locHost Local host.
     * @param nodeId Node ID.
     * @param marshaller Marshaller.
     * @param jmx Jmx Bean Server.
     * @param home Ignite home.
     */
    public IgniteMock(
        String name, String locHost, UUID nodeId, Marshaller marshaller, MBeanServer jmx, String home) {
        this.locHost = locHost;
        this.nodeId = nodeId;
        this.marshaller = marshaller;
        this.jmx = jmx;
        this.home = home;
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration configuration() {
        if (staticCfg != null)
            return staticCfg;

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setMarshaller(marshaller);
        cfg.setNodeId(nodeId);
        cfg.setMBeanServer(jmx);
        cfg.setIgniteHome(home);
        cfg.setLocalHost(locHost);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public IgniteCluster cluster() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute(ClusterGroup grp) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message(ClusterGroup prj) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events(ClusterGroup grp) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services(ClusterGroup grp) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup grp) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> cache(@Nullable String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg,
        @Nullable NearCacheConfiguration<K, V> nearCfg) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createNearCache(String cacheName, NearCacheConfiguration<K, V> nearCfg) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg, NearCacheConfiguration<K, V> nearCfg) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateNearCache(@Nullable String cacheName, NearCacheConfiguration<K, V> nearCfg) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(String cacheName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String cacheName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteFileSystem fileSystem(String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFileSystem> fileSystems() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() {}

    @Nullable @Override public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create) {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteAtomicReference<T> atomicReference(String name,
        @Nullable T initVal,
        boolean create)
    {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name,
        @Nullable T initVal,
        @Nullable S initStamp,
        boolean create)
    {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteCountDownLatch countDownLatch(String name,
        int cnt,
        boolean autoDel,
        boolean create)
    {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteQueue<T> queue(String name,
        int cap,
        CollectionConfiguration cfg)
    {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteSet<T> set(String name,
        CollectionConfiguration cfg)
    {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K> Affinity<K> affinity(String cacheName) {
        return null;
    }

    /**
     * @param staticCfg Configuration.
     */
    public void setStaticCfg(IgniteConfiguration staticCfg) {
        this.staticCfg = staticCfg;
    }
}
