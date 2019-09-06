package org.apache.ignite.internal.processors.security.sandbox;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import javax.cache.CacheException;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataStorageMetrics;
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
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.PersistenceMetrics;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteInstance implements Ignite {
    /**
     *
     */
    private final Ignite original;

    /**
     *
     */
    public IgniteInstance(Ignite original) {
        this.original = original;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return original.name();
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log() {
        return original.log();
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration configuration() {
        return original.configuration();
    }

    /** {@inheritDoc} */
    @Override public IgniteCluster cluster() {
        return original.cluster();
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute() {
        return original.compute();
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute(ClusterGroup grp) {
        return original.compute(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message() {
        return original.message();
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message(ClusterGroup grp) {
        return original.message(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events() {
        return original.events();
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events(ClusterGroup grp) {
        return original.events(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services() {
        return original.services();
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services(ClusterGroup grp) {
        return original.services(grp);
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        return original.executorService();
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup grp) {
        return original.executorService(grp);
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return original.version();
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        return original.scheduler();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(
        CacheConfiguration<K, V> cacheCfg) throws CacheException {
        return original.createCache(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteCache> createCaches(
        Collection<CacheConfiguration> cacheCfgs) throws CacheException {
        return original.createCaches(cacheCfgs);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(String cacheName) throws CacheException {
        return original.createCache(cacheName);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(
        CacheConfiguration<K, V> cacheCfg) throws CacheException {
        return original.getOrCreateCache(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) throws CacheException {
        return original.getOrCreateCache(cacheName);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteCache> getOrCreateCaches(
        Collection<CacheConfiguration> cacheCfgs) throws CacheException {
        return original.getOrCreateCaches(cacheCfgs);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) throws CacheException {
        original.addCacheConfiguration(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(
        CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) throws CacheException {
        return original.createCache(cacheCfg, nearCfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(
        CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) throws CacheException {
        return original.getOrCreateCache(cacheCfg, nearCfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createNearCache(String cacheName,
        NearCacheConfiguration<K, V> nearCfg) throws CacheException {
        return original.createNearCache(cacheName, nearCfg);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateNearCache(String cacheName,
        NearCacheConfiguration<K, V> nearCfg) throws CacheException {
        return original.getOrCreateNearCache(cacheName, nearCfg);
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) throws CacheException {
        original.destroyCache(cacheName);
    }

    /** {@inheritDoc} */
    @Override public void destroyCaches(Collection<String> cacheNames) throws CacheException {
        original.destroyCaches(cacheNames);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> cache(String name) throws CacheException {
        return original.cache(name);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() {
        return original.cacheNames();
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        return original.transactions();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataStreamer<K, V> dataStreamer(String cacheName) throws IllegalStateException {
        return original.dataStreamer(cacheName);
    }

    /** {@inheritDoc} */
    @Override public IgniteFileSystem fileSystem(String name) throws IllegalArgumentException {
        return original.fileSystem(name);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFileSystem> fileSystems() {
        return original.fileSystems();
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicSequence atomicSequence(String name, long initVal,
        boolean create) throws IgniteException {
        return original.atomicSequence(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicSequence atomicSequence(String name,
        AtomicConfiguration cfg, long initVal, boolean create) throws IgniteException {
        return original.atomicSequence(name, cfg, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteException {
        return original.atomicLong(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicLong atomicLong(String name,
        AtomicConfiguration cfg, long initVal, boolean create) throws IgniteException {
        return original.atomicLong(name, cfg, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteAtomicReference<T> atomicReference(String name,
        @Nullable T initVal, boolean create) throws IgniteException {
        return original.atomicReference(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteAtomicReference<T> atomicReference(String name,
        AtomicConfiguration cfg, @Nullable T initVal,
        boolean create) throws IgniteException {
        return original.atomicReference(name, cfg, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name,
        @Nullable T initVal, @Nullable S initStamp, boolean create) throws IgniteException {
        return original.atomicStamped(name, initVal, initStamp, create);
    }

    /** {@inheritDoc} */
    @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name,
        AtomicConfiguration cfg, @Nullable T initVal,
        @Nullable S initStamp, boolean create) throws IgniteException {
        return original.atomicStamped(name, cfg, initVal, initStamp, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteCountDownLatch countDownLatch(String name, int cnt, boolean autoDel,
        boolean create) throws IgniteException {
        return original.countDownLatch(name, cnt, autoDel, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteSemaphore semaphore(String name, int cnt, boolean failoverSafe,
        boolean create) throws IgniteException {
        return original.semaphore(name, cnt, failoverSafe, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteLock reentrantLock(String name, boolean failoverSafe, boolean fair,
        boolean create) throws IgniteException {
        return original.reentrantLock(name, failoverSafe, fair, create);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteQueue<T> queue(String name, int cap,
        @Nullable CollectionConfiguration cfg) throws IgniteException {
        return original.queue(name, cap, cfg);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteSet<T> set(String name,
        @Nullable CollectionConfiguration cfg) throws IgniteException {
        return original.set(name, cfg);
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        return original.plugin(name);
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        return original.binary();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        original.close();
    }

    /** {@inheritDoc} */
    @Override public <K> Affinity<K> affinity(String cacheName) {
        return original.affinity(cacheName);
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override public boolean active() {
        return original.active();
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override public void active(boolean active) {
        original.active(active);
    }

    /** {@inheritDoc} */
    @Override public void resetLostPartitions(Collection<String> cacheNames) {
        original.resetLostPartitions(cacheNames);
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override public Collection<MemoryMetrics> memoryMetrics() {
        return original.memoryMetrics();
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override public @Nullable MemoryMetrics memoryMetrics(String memPlcName) {
        return original.memoryMetrics(memPlcName);
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override public PersistenceMetrics persistentStoreMetrics() {
        return original.persistentStoreMetrics();
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRegionMetrics> dataRegionMetrics() {
        return original.dataRegionMetrics();
    }

    /** {@inheritDoc} */
    @Override public @Nullable DataRegionMetrics dataRegionMetrics(
        String memPlcName) {
        return original.dataRegionMetrics(memPlcName);
    }

    /** {@inheritDoc} */
    @Override public DataStorageMetrics dataStorageMetrics() {
        return original.dataStorageMetrics();
    }
}
