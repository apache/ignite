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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataRegionMetricsAdapter;
import org.apache.ignite.DataStorageMetricsAdapter;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.PersistenceMetrics;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.cache.GridCacheUtilityKey;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.hadoop.Hadoop;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import javax.cache.CacheException;

/**
 * Mocked Ignite implementation for IGFS tests.
 */
public class IgfsIgniteMock implements IgniteEx {
    /** Name. */
    private final String name;

    /** IGFS. */
    private final IgniteFileSystem igfs;

    /**
     * Constructor.
     *
     * @param igfs IGFS instance.
     */
    public IgfsIgniteMock(@Nullable String name, IgniteFileSystem igfs) {
        this.name = name;
        this.igfs = igfs;
    }

    /** {@inheritDoc} */
    @Override public <K extends GridCacheUtilityKey, V> IgniteInternalCache<K, V> utilityCache() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> IgniteInternalCache<K, V> cachex(@Nullable String name) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Collection<IgniteInternalCache<?, ?>> cachesx(
        @Nullable IgnitePredicate<? super IgniteInternalCache<?, ?>>... p) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean eventUserRecordable(int type) {
        throwUnsupported();

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean allEventsUserRecordable(int[] types) {
        throwUnsupported();

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isJmxRemoteEnabled() {
        throwUnsupported();

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isRestartEnabled() {
        throwUnsupported();

        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFileSystem igfsx(String name) {
        return F.eq(name, igfs.name()) ? igfs : null;
    }

    /** {@inheritDoc} */
    @Override public Hadoop hadoop() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteClusterEx cluster() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String latestVersion() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public GridKernalContext context() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isRebalanceEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void rebalanceEnabled(boolean rebalanceEnabled) {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration configuration() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute(ClusterGroup grp) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message(ClusterGroup grp) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events(ClusterGroup grp) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services(ClusterGroup grp) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup grp) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteCache> createCaches(Collection<CacheConfiguration> cacheCfgs) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(String cacheName) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteCache> getOrCreateCaches(Collection<CacheConfiguration> cacheCfgs) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteBiTuple<IgniteCache<K, V>, Boolean> getOrCreateCache0(
        CacheConfiguration<K, V> cacheCfg, boolean sql) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean destroyCache0(String cacheName, boolean sql) throws CacheException {
        throwUnsupported();

        return false;
    }

    /** {@inheritDoc} */
    @Override public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createNearCache(@Nullable String cacheName,
        NearCacheConfiguration<K, V> nearCfg) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateNearCache(@Nullable String cacheName,
        NearCacheConfiguration<K, V> nearCfg) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public void destroyCaches(Collection<String> cacheNames) {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> cache(@Nullable String name) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String cacheName) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteFileSystem fileSystem(String name) {
        IgniteFileSystem res = igfsx(name);

        if (res == null)
            throw new IllegalArgumentException("IGFS is not configured: " + name);

        return res;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFileSystem> fileSystems() {
        return Collections.singleton(igfs);
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create)
        throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicSequence atomicSequence(String name, AtomicConfiguration cfg, long initVal,
        boolean create) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicLong atomicLong(String name, AtomicConfiguration cfg, long initVal,
        boolean create) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteAtomicReference<T> atomicReference(String name, @Nullable T initVal, boolean create)
        throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteAtomicReference<T> atomicReference(String name, AtomicConfiguration cfg,
        @Nullable T initVal, boolean create) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, @Nullable T initVal,
        @Nullable S initStamp, boolean create) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, AtomicConfiguration cfg,
        @Nullable T initVal, @Nullable S initStamp, boolean create) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteCountDownLatch countDownLatch(String name, int cnt, boolean autoDel, boolean create)
        throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteSemaphore semaphore(String name, int cnt, boolean failoverSafe, boolean create)
        throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteLock reentrantLock(String name, boolean failoverSafe, boolean fair, boolean create)
        throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteQueue<T> queue(String name, int cap, @Nullable CollectionConfiguration cfg)
        throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteSet<T> set(String name, @Nullable CollectionConfiguration cfg) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public <K> Affinity<K> affinity(String cacheName) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean active() {
        throwUnsupported();

        return true;
    }

    /** {@inheritDoc} */
    @Override public void active(boolean active) {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public void resetLostPartitions(Collection<String> cacheNames) {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRegionMetrics> dataRegionMetrics() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DataRegionMetrics dataRegionMetrics(String memPlcName) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public DataStorageMetrics dataStorageMetrics() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<MemoryMetrics> memoryMetrics() {
        return DataRegionMetricsAdapter.collectionOf(dataRegionMetrics());
    }

    /** {@inheritDoc} */
    @Nullable @Override public MemoryMetrics memoryMetrics(String memPlcName) {
        return DataRegionMetricsAdapter.valueOf(dataRegionMetrics(memPlcName));
    }

    /** {@inheritDoc} */
    @Override public PersistenceMetrics persistentStoreMetrics() {
        return DataStorageMetricsAdapter.valueOf(dataStorageMetrics());
    }

    /**
     * Throw {@link UnsupportedOperationException}.
     */
    private static void throwUnsupported() {
        throw new UnsupportedOperationException("Should not be called!");
    }
}
