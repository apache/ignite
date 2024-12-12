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

package org.apache.ignite.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.cache.CacheException;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteEncryption;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTransactionsImpl;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.metric.IgniteMetrics;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.apache.ignite.spi.tracing.TracingConfigurationManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Ignite instance aware of application attributes set with {@link Ignite#withApplicationAttributes(Map)}. */
public class IgniteApplicationAttributesAware implements Ignite {
    /** */
    private final Ignite delegate;

    /** Application attributes. */
    private final Map<String, String> attrs;

    /** Stores caches configured with the application attributes. */
    private final Map<String, IgniteCache<?, ?>> appAttrCaches = new ConcurrentHashMap<>();

    /**
     * @param delegate Parent Ignite instance.
     * @param attrs Application attributes.
     */
    public IgniteApplicationAttributesAware(Ignite delegate, Map<String, String> attrs) {
        A.notNull(attrs, "application attributes");

        this.delegate = delegate;
        this.attrs = new HashMap<>(attrs);
    }

    /**
     * Set application attributes to all returned caches.
     *
     * @param cache Cache, or {@code null}.
     * @return Cache with application attributes.
     */
    private <K, V> IgniteCache<K, V> withApplicationAttributes(@Nullable IgniteCache<K, V> cache) {
        if (cache == null)
            return null;

        return (IgniteCache<K, V>)appAttrCaches
            .computeIfAbsent(cache.getName(), c -> ((IgniteCacheProxy<K, V>)cache).withApplicationAttributes(attrs));
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg) throws CacheException {
        return withApplicationAttributes(delegate.createCache(cacheCfg));
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteCache> createCaches(Collection<CacheConfiguration> cacheCfgs) throws CacheException {
        return delegate.createCaches(cacheCfgs)
            .stream()
            .map(c -> withApplicationAttributes(c))
            .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(String cacheName) throws CacheException {
        return withApplicationAttributes(delegate.createCache(cacheName));
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) throws CacheException {
        IgniteCache<K, V> cache = (IgniteCache<K, V>)appAttrCaches.get(cacheCfg.getName());

        if (cache != null)
            return cache;

        return withApplicationAttributes(delegate.getOrCreateCache(cacheCfg));
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) throws CacheException {
        IgniteCache<K, V> cache = (IgniteCache<K, V>)appAttrCaches.get(cacheName);

        if (cache != null)
            return cache;

        return withApplicationAttributes(delegate.getOrCreateCache(cacheName));
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteCache> getOrCreateCaches(Collection<CacheConfiguration> cacheCfgs) throws CacheException {
        return delegate.getOrCreateCaches(cacheCfgs)
            .stream()
            .map(c -> withApplicationAttributes(c))
            .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(
        CacheConfiguration<K, V> cacheCfg, NearCacheConfiguration<K, V> nearCfg
    ) throws CacheException {
        return withApplicationAttributes(delegate.createCache(cacheCfg, nearCfg));
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(
        CacheConfiguration<K, V> cacheCfg, NearCacheConfiguration<K, V> nearCfg
    ) throws CacheException {
        return withApplicationAttributes(delegate.getOrCreateCache(cacheCfg, nearCfg));
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createNearCache(
        String cacheName, NearCacheConfiguration<K, V> nearCfg
    ) throws CacheException {
        return withApplicationAttributes(delegate.createNearCache(cacheName, nearCfg));
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateNearCache(
        String cacheName, NearCacheConfiguration<K, V> nearCfg
    ) throws CacheException {
        return withApplicationAttributes(delegate.getOrCreateNearCache(cacheName, nearCfg));
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> cache(String name) throws CacheException {
        IgniteCache<K, V> cache = (IgniteCache<K, V>)appAttrCaches.get(name);

        if (cache != null)
            return cache;

        return withApplicationAttributes(delegate.cache(name));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return delegate.name();
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log() {
        return delegate.log();
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration configuration() {
        return delegate.configuration();
    }

    /** {@inheritDoc} */
    @Override public IgniteCluster cluster() {
        return delegate.cluster();
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute() {
        return delegate.compute();
    }

    /** {@inheritDoc} */
    @Override public IgniteMetrics metrics() {
        return delegate.metrics();
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute(ClusterGroup grp) {
        return delegate.compute(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message() {
        return delegate.message();
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message(ClusterGroup grp) {
        return delegate.message(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events() {
        return delegate.events();
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events(ClusterGroup grp) {
        return delegate.events(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services() {
        return delegate.services();
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services(ClusterGroup grp) {
        return delegate.services(grp);
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        return delegate.executorService();
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup grp) {
        return delegate.executorService(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return delegate.version();
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        return delegate.scheduler();
    }

    /** {@inheritDoc} */
    @Override public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) throws CacheException {
        delegate.addCacheConfiguration(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) throws CacheException {
        delegate.destroyCache(cacheName);

        appAttrCaches.remove(cacheName);
    }

    /** {@inheritDoc} */
    @Override public void destroyCaches(Collection<String> cacheNames) throws CacheException {
        delegate.destroyCaches(cacheNames);

        for (String c: cacheNames)
            appAttrCaches.remove(c);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() {
        return delegate.cacheNames();
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        IgniteTransactionsImpl<?, ?> txs = (IgniteTransactionsImpl<?, ?>)delegate.transactions();

        return txs.withApplicationAttributes(attrs);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataStreamer<K, V> dataStreamer(String cacheName) throws IllegalStateException {
        return delegate.dataStreamer(cacheName);
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create) throws IgniteException {
        return delegate.atomicSequence(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicSequence atomicSequence(
        String name, AtomicConfiguration cfg, long initVal, boolean create
    ) throws IgniteException {
        return delegate.atomicSequence(name, cfg, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteException {
        return delegate.atomicLong(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicLong atomicLong(
        String name, AtomicConfiguration cfg, long initVal, boolean create
    ) throws IgniteException {
        return delegate.atomicLong(name, cfg, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteAtomicReference<T> atomicReference(
        String name, @Nullable T initVal, boolean create
    ) throws IgniteException {
        return delegate.atomicReference(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteAtomicReference<T> atomicReference(
        String name, AtomicConfiguration cfg, @Nullable T initVal, boolean create
    ) throws IgniteException {
        return delegate.atomicReference(name, cfg, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(
        String name, @Nullable T initVal, @Nullable S initStamp, boolean create
    ) throws IgniteException {
        return delegate.atomicStamped(name, initVal, initStamp, create);
    }

    /** {@inheritDoc} */
    @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(
        String name, AtomicConfiguration cfg, @Nullable T initVal, @Nullable S initStamp, boolean create
    ) throws IgniteException {
        return delegate.atomicStamped(name, cfg, initVal, initStamp, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteCountDownLatch countDownLatch(
        String name, int cnt, boolean autoDel, boolean create
    ) throws IgniteException {
        return delegate.countDownLatch(name, cnt, autoDel, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteSemaphore semaphore(String name, int cnt, boolean failoverSafe, boolean create) throws IgniteException {
        return delegate.semaphore(name, cnt, failoverSafe, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteLock reentrantLock(String name, boolean failoverSafe, boolean fair, boolean create) throws IgniteException {
        return delegate.reentrantLock(name, failoverSafe, fair, create);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteQueue<T> queue(String name, int cap, @Nullable CollectionConfiguration cfg) throws IgniteException {
        return delegate.queue(name, cap, cfg);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteSet<T> set(String name, @Nullable CollectionConfiguration cfg) throws IgniteException {
        return delegate.set(name, cfg);
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        return delegate.plugin(name);
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        return delegate.binary();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        delegate.close();
    }

    /** {@inheritDoc} */
    @Override public <K> Affinity<K> affinity(String cacheName) {
        return delegate.affinity(cacheName);
    }

    /** {@inheritDoc} */
    @Override public boolean active() {
        return delegate.active();
    }

    /** {@inheritDoc} */
    @Override public void active(boolean active) {
        delegate.active(active);
    }

    /** {@inheritDoc} */
    @Override public void resetLostPartitions(Collection<String> cacheNames) {
        delegate.resetLostPartitions(cacheNames);
    }

    /** {@inheritDoc} */
    @Override public Collection<MemoryMetrics> memoryMetrics() {
        return delegate.memoryMetrics();
    }

    /** {@inheritDoc} */
    @Override public @Nullable MemoryMetrics memoryMetrics(String dataRegionName) {
        return delegate.memoryMetrics(dataRegionName);
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRegionMetrics> dataRegionMetrics() {
        return delegate.dataRegionMetrics();
    }

    /** {@inheritDoc} */
    @Override public @Nullable DataRegionMetrics dataRegionMetrics(String memPlcName) {
        return delegate.dataRegionMetrics(memPlcName);
    }

    /** {@inheritDoc} */
    @Override public IgniteEncryption encryption() {
        return delegate.encryption();
    }

    /** {@inheritDoc} */
    @Override public IgniteSnapshot snapshot() {
        return delegate.snapshot();
    }

    /** {@inheritDoc} */
    @Override public @NotNull TracingConfigurationManager tracingConfiguration() {
        return delegate.tracingConfiguration();
    }

    /** */
    @Override public Ignite withApplicationAttributes(Map<String, String> attrs) {
        if (this.attrs.equals(attrs))
            return this;

        return delegate.withApplicationAttributes(attrs);
    }
}
