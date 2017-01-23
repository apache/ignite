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

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.hadoop.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.*;
import org.jetbrains.annotations.*;
import java.util.*;
import java.util.concurrent.*;

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
    @Nullable @Override public <K, V> IgniteInternalCache<K, V> cachex() {
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
    @Nullable @Override public IgniteFileSystem igfsx(@Nullable String name) {
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
    @Override public IgniteMath math() {
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
    @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteException {
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
    @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, @Nullable T initVal,
        @Nullable S initStamp, boolean create) throws IgniteException {
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

    /**
     * Throw {@link UnsupportedOperationException}.
     */
    private static void throwUnsupported() {
        throw new UnsupportedOperationException("Should not be called!");
    }
}
