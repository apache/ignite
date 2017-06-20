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

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import javax.cache.CacheException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.jetbrains.annotations.Nullable;

/**
 * Main entry-point for all Ignite APIs.
 * You can obtain an instance of {@code Ignite} through {@link Ignition#ignite()},
 * or for named grids you can use {@link Ignition#ignite(String)}. Note that you
 * can have multiple instances of {@code Ignite} running in the same VM by giving
 * each instance a different name.
 * <p>
 * Ignite provides the following functionality:
 * <ul>
 * <li>{@link IgniteCluster} - clustering functionality.</li>
 * <li>{@link IgniteCache} - functionality for in-memory distributed cache, including SQL, TEXT, and Predicate-based queries.</li>
 * <li>{@link IgniteTransactions} - distributed ACID-compliant transactions.</li>
 * <li>{@link IgniteDataStreamer} - functionality for streaming large amounts of data into cache.</li>
 * <li>{@link IgniteCompute} - functionality for executing tasks and closures on all grid nodes (inherited form {@link ClusterGroup}).</li>
 * <li>{@link IgniteServices} - distributed service grid functionality (e.g. singletons on the cluster).</li>
 * <li>{@link IgniteMessaging} - functionality for topic-based message exchange on all grid nodes (inherited form {@link ClusterGroup}).</li>
 * <li>{@link IgniteEvents} - functionality for querying and listening to events on all grid nodes  (inherited form {@link ClusterGroup}).</li>
 * <li>{@link ExecutorService} - distributed thread pools.</li>
 * <li>{@link IgniteAtomicLong} - distributed atomic long.</li>
 * <li>{@link IgniteAtomicReference} - distributed atomic reference.</li>
 * <li>{@link IgniteAtomicSequence} - distributed atomic sequence.</li>
 * <li>{@link IgniteAtomicStamped} - distributed atomic stamped reference.</li>
 * <li>{@link IgniteCountDownLatch} - distributed count down latch.</li>
 * <li>{@link IgniteQueue} - distributed blocking queue.</li>
 * <li>{@link IgniteSet} - distributed concurrent set.</li>
 * <li>{@link IgniteScheduler} - functionality for scheduling jobs using UNIX Cron syntax.</li>
 * <li>{@link IgniteFileSystem} - functionality for distributed Hadoop-compliant in-memory file system and map-reduce.</li>
 * </ul>
 */
public interface Ignite extends AutoCloseable {
    /**
     * Gets the name of the Ignite instance.
     * The name allows having multiple Ignite instances with different names within the same Java VM.
     * <p>
     * If default Ignite instance is used, then {@code null} is returned.
     * Refer to {@link Ignition} documentation for information on how to start named ignite Instances.
     *
     * @return Name of the Ignite instance, or {@code null} for default Ignite instance.
     */
    public String name();

    /**
     * Gets grid's logger.
     *
     * @return Grid's logger.
     */
    public IgniteLogger log();

    /**
     * Gets the configuration of this Ignite instance.
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
     */
    public IgniteConfiguration configuration();

    /**
     * Gets an instance of {@link IgniteCluster} interface.
     *
     * @return Instance of {@link IgniteCluster} interface.
     */
    public IgniteCluster cluster();

    /**
     * Gets {@code compute} facade over all cluster nodes started in server mode.
     *
     * @return Compute instance over all cluster nodes started in server mode.
     */
    public IgniteCompute compute();

    /**
     * Gets {@code compute} facade over the specified cluster group. All operations
     * on the returned {@link IgniteCompute} instance will only include nodes from
     * this cluster group.
     *
     * @param grp Cluster group.
     * @return Compute instance over given cluster group.
     */
    public IgniteCompute compute(ClusterGroup grp);

    /**
     * Gets {@code messaging} facade over all cluster nodes.
     *
     * @return Messaging instance over all cluster nodes.
     */
    public IgniteMessaging message();

    /**
     * Gets {@code messaging} facade over nodes within the cluster group.  All operations
     * on the returned {@link IgniteMessaging} instance will only include nodes from
     * the specified cluster group.
     *
     * @param grp Cluster group.
     * @return Messaging instance over given cluster group.
     */
    public IgniteMessaging message(ClusterGroup grp);

    /**
     * Gets {@code events} facade over all cluster nodes.
     *
     * @return Events instance over all cluster nodes.
     */
    public IgniteEvents events();

    /**
     * Gets {@code events} facade over nodes within the cluster group. All operations
     * on the returned {@link IgniteEvents} instance will only include nodes from
     * the specified cluster group.
     *
     * @param grp Cluster group.
     * @return Events instance over given cluster group.
     */
    public IgniteEvents events(ClusterGroup grp);

    /**
     * Gets {@code services} facade over all cluster nodes started in server mode.
     *
     * @return Services facade over all cluster nodes started in server mode.
     */
    public IgniteServices services();

    /**
     * Gets {@code services} facade over nodes within the cluster group. All operations
     * on the returned {@link IgniteMessaging} instance will only include nodes from
     * the specified cluster group.
     *
     * @param grp Cluster group.
     * @return {@code Services} functionality over given cluster group.
     */
    public IgniteServices services(ClusterGroup grp);

    /**
     * Creates a new {@link ExecutorService} which will execute all submitted
     * {@link Callable} and {@link Runnable} jobs on all cluster nodes.
     * This essentially creates a <b><i>Distributed Thread Pool</i></b> that can
     * be used as a replacement for local thread pools.
     *
     * @return Grid-enabled {@code ExecutorService}.
     */
    public ExecutorService executorService();

    /**
     * Creates a new {@link ExecutorService} which will execute all submitted
     * {@link Callable} and {@link Runnable} jobs on nodes in the specified cluster group.
     * This essentially creates a <b><i>Distributed Thread Pool</i></b> that can be used as a
     * replacement for local thread pools.
     *
     * @param grp Cluster group.
     * @return {@link ExecutorService} which will execute jobs on nodes in given cluster group.
     */
    public ExecutorService executorService(ClusterGroup grp);

    /**
     * Gets Ignite version.
     *
     * @return Ignite version.
     */
    public IgniteProductVersion version();

    /**
     * Gets an instance of cron-based scheduler.
     *
     * @return Instance of scheduler.
     */
    public IgniteScheduler scheduler();

    /**
     * Dynamically starts new cache with the given cache configuration.
     * <p>
     * If local node is an affinity node, this method will return the instance of started cache.
     * Otherwise, it will create a client cache on local node.
     * <p>
     * If a cache with the same name already exists in the grid, an exception will be thrown regardless
     * whether the given configuration matches the configuration of the existing cache or not.
     *
     * @param cacheCfg Cache configuration to use.
     * @return Instance of started cache.
     * @throws CacheException If a cache with the same name already exists or other error occurs.
     */
    public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg) throws CacheException;

    /**
     * Dynamically starts new caches with the given cache configurations.
     * <p>
     * If local node is an affinity node, this method will return the instance of started caches.
     * Otherwise, it will create a client caches on local node.
     * <p>
     * If for one of configurations a cache with the same name already exists in the grid, an exception will be thrown regardless
     * whether the given configuration matches the configuration of the existing cache or not.
     *
     * @param cacheCfgs Collection of cache configuration to use.
     * @return Collection of instances of started caches.
     * @throws CacheException If one of created caches exists or other error occurs.
     */
    public Collection<IgniteCache> createCaches(Collection<CacheConfiguration> cacheCfgs) throws CacheException;

    /**
     * Dynamically starts new cache using template configuration.
     * <p>
     * If local node is an affinity node, this method will return the instance of started cache.
     * Otherwise, it will create a client cache on local node.
     * <p>
     * If a cache with the same name already exists in the grid, an exception will be thrown.
     *
     * @param cacheName Cache name.
     * @return Instance of started cache.
     * @throws CacheException If a cache with the same name already exists or other error occurs.
     */
    public <K, V> IgniteCache<K, V> createCache(String cacheName) throws CacheException;

    /**
     * Gets existing cache with the given name or creates new one with the given configuration.
     * <p>
     * If a cache with the same name already exist, this method will not check that the given
     * configuration matches the configuration of existing cache and will return an instance
     * of the existing cache.
     *
     * @param cacheCfg Cache configuration to use.
     * @return Existing or newly created cache.
     * @throws CacheException If error occurs.
     */
    public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) throws CacheException;

    /**
     * Gets existing cache with the given name or creates new one using template configuration.
     *
     * @param cacheName Cache name.
     * @return Existing or newly created cache.
     * @throws CacheException If error occurs.
     */
    public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) throws CacheException;

    /**
     * Gets existing caches with the given name or created one with the given configuration.
     * <p>
     * If a cache with the same name already exist, this method will not check that the given
     * configuration matches the configuration of existing cache and will return an instance
     * of the existing cache.
     *
     * @param cacheCfgs Collection of cache configuration to use.
     * @return Collection of existing or newly created caches.
     * @throws CacheException If error occurs.
     */
    public Collection<IgniteCache> getOrCreateCaches(Collection<CacheConfiguration> cacheCfgs) throws CacheException;

    /**
     * Adds cache configuration template.
     *
     * @param cacheCfg Cache configuration template.
     * @throws CacheException If error occurs.
     */
    public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) throws CacheException;

    /**
     * Dynamically starts new cache with the given cache configuration.
     * <p>
     * If local node is an affinity node, this method will return the instance of started cache.
     * Otherwise, it will create a near cache with the given configuration on local node.
     * <p>
     * If a cache with the same name already exists in the grid, an exception will be thrown regardless
     * whether the given configuration matches the configuration of the existing cache or not.
     *
     * @param cacheCfg Cache configuration to use.
     * @param nearCfg Near cache configuration to use on local node in case it is not an
     *      affinity node.
     * @throws CacheException If a cache with the same name already exists or other error occurs.
     * @return Instance of started cache.
     */
    public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) throws CacheException;

    /**
     * Gets existing cache with the given cache configuration or creates one if it does not exist.
     * <p>
     * If a cache with the same name already exist, this method will not check that the given
     * configuration matches the configuration of existing cache and will return an instance
     * of the existing cache.
     * <p>
     * If local node is not an affinity node and a client cache without near cache has been already started
     * on this node, an exception will be thrown.
     *
     * @param cacheCfg Cache configuration.
     * @param nearCfg Near cache configuration for client.
     * @return {@code IgniteCache} instance.
     * @throws CacheException If error occurs.
     */
    public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) throws CacheException;

    /**
     * Starts a near cache on local node if cache was previously started with one of the
     * {@link #createCache(CacheConfiguration)} or {@link #createCache(CacheConfiguration, NearCacheConfiguration)}
     * methods.
     *
     * @param cacheName Cache name.
     * @param nearCfg Near cache configuration.
     * @return Cache instance.
     * @throws CacheException If error occurs.
     */
    public <K, V> IgniteCache<K, V> createNearCache(String cacheName, NearCacheConfiguration<K, V> nearCfg)
        throws CacheException;

    /**
     * Gets existing near cache with the given name or creates a new one.
     *
     * @param cacheName Cache name.
     * @param nearCfg Near configuration.
     * @return {@code IgniteCache} instance.
     * @throws CacheException If error occurs.
     */
    public <K, V> IgniteCache<K, V> getOrCreateNearCache(String cacheName, NearCacheConfiguration<K, V> nearCfg)
        throws CacheException;

    /**
     * Stops dynamically started cache.
     *
     * @param cacheName Cache name to stop.
     * @throws CacheException If error occurs.
     */
    public void destroyCache(String cacheName) throws CacheException;

    /**
     * Stops dynamically started caches.
     *
     * @param cacheNames Collection of cache names to stop.
     * @throws CacheException If error occurs.
     */
    public void destroyCaches(Collection<String> cacheNames) throws CacheException;

    /**
     * Gets an instance of {@link IgniteCache} API. {@code IgniteCache} is a fully-compatible
     * implementation of {@code JCache (JSR 107)} specification.
     *
     * @param name Cache name.
     * @return Instance of the cache for the specified name.
     * @throws CacheException If error occurs.
     */
    public <K, V> IgniteCache<K, V> cache(String name) throws CacheException;

    /**
     * Gets the collection of names of currently available caches.
     *
     * Collection may contain {@code null} as a value for a cache name. Refer to {@link CacheConfiguration#getName()}
     * for more info.
     *
     * @return Collection of names of currently available caches or an empty collection if no caches are available.
     */
    public Collection<String> cacheNames();

    /**
     * Gets grid transactions facade.
     *
     * @return Grid transactions facade.
     */
    public IgniteTransactions transactions();

    /**
     * Gets a new instance of data streamer associated with given cache name. Data streamer
     * is responsible for loading external data into in-memory data grid. For more information
     * refer to {@link IgniteDataStreamer} documentation.
     *
     * @param cacheName Cache name.
     * @return Data streamer.
     * @throws IllegalStateException If node is stopping.
     */
    public <K, V> IgniteDataStreamer<K, V> dataStreamer(String cacheName) throws IllegalStateException;

    /**
     * Gets an instance of IGFS (Ignite In-Memory File System). If one is not
     * configured then {@link IllegalArgumentException} will be thrown.
     * <p>
     * IGFS is fully compliant with Hadoop {@code FileSystem} APIs and can
     * be plugged into Hadoop installations. For more information refer to
     * documentation on Hadoop integration shipped with Ignite.
     *
     * @param name IGFS name.
     * @return IGFS instance.
     * @throws IllegalArgumentException If IGFS with such name is not configured.
     */
    public IgniteFileSystem fileSystem(String name) throws IllegalArgumentException;

    /**
     * Gets all instances of IGFS (Ignite In-Memory File System).
     *
     * @return Collection of IGFS instances.
     */
    public Collection<IgniteFileSystem> fileSystems();

    /**
     * Will get an atomic sequence from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}. It will use configuration from {@link IgniteConfiguration#getAtomicConfiguration()}.
     *
     * @param name Sequence name.
     * @param initVal Initial value for sequence. Ignored if {@code create} flag is {@code false}.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Sequence for the given name.
     * @throws IgniteException If sequence could not be fetched or created.
     */
    public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create)
        throws IgniteException;

    /**
     * Will get an atomic sequence from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
     *
     * @param name Sequence name.
     * @param cfg Configuration.
     * @param initVal Initial value for sequence. Ignored if {@code create} flag is {@code false}.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Sequence for the given name.
     * @throws IgniteException If sequence could not be fetched or created.
     */
    public IgniteAtomicSequence atomicSequence(String name, AtomicConfiguration cfg, long initVal, boolean create)
        throws IgniteException;

    /**
     * Will get a atomic long from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
     *
     * @param name Name of atomic long.
     * @param initVal Initial value for atomic long. Ignored if {@code create} flag is {@code false}.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Atomic long.
     * @throws IgniteException If atomic long could not be fetched or created.
     */
    public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteException;

    /**
     * Will get a atomic long from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
     *
     * @param name Name of atomic long.
     * @param cfg Configuration.
     * @param initVal Initial value for atomic long. Ignored if {@code create} flag is {@code false}.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Atomic long.
     * @throws IgniteException If atomic long could not be fetched or created.
     */
    public IgniteAtomicLong atomicLong(String name, AtomicConfiguration cfg, long initVal, boolean create) throws IgniteException;

    /**
     * Will get a atomic reference from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}. It will use configuration from {@link IgniteConfiguration#getAtomicConfiguration()}.
     *
     * @param name Atomic reference name.
     * @param initVal Initial value for atomic reference. Ignored if {@code create} flag is {@code false}.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Atomic reference for the given name.
     * @throws IgniteException If atomic reference could not be fetched or created.
     */
    public <T> IgniteAtomicReference<T> atomicReference(String name, @Nullable T initVal, boolean create)
        throws IgniteException;

    /**
     * Will get a atomic reference from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
     *
     * @param name Atomic reference name.
     * @param cfg Configuration.
     * @param initVal Initial value for atomic reference. Ignored if {@code create} flag is {@code false}.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Atomic reference for the given name.
     * @throws IgniteException If atomic reference could not be fetched or created.
     */
    public <T> IgniteAtomicReference<T> atomicReference(String name, AtomicConfiguration cfg, @Nullable T initVal, boolean create)
        throws IgniteException;

    /**
     * Will get a atomic stamped from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
     *
     * @param name Atomic stamped name.
     * @param initVal Initial value for atomic stamped. Ignored if {@code create} flag is {@code false}.
     * @param initStamp Initial stamp for atomic stamped. Ignored if {@code create} flag is {@code false}.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Atomic stamped for the given name.
     * @throws IgniteException If atomic stamped could not be fetched or created.
     */
    public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, @Nullable T initVal,
        @Nullable S initStamp, boolean create) throws IgniteException;

    /**
     * Will get a atomic stamped from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
     *
     * @param name Atomic stamped name.
     * @param cfg Configuration.
     * @param initVal Initial value for atomic stamped. Ignored if {@code create} flag is {@code false}.
     * @param initStamp Initial stamp for atomic stamped. Ignored if {@code create} flag is {@code false}.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Atomic stamped for the given name.
     * @throws IgniteException If atomic stamped could not be fetched or created.
     */
    public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, AtomicConfiguration cfg, @Nullable T initVal,
        @Nullable S initStamp, boolean create) throws IgniteException;

    /**
     * Gets or creates count down latch. If count down latch is not found in cache and {@code create} flag
     * is {@code true}, it is created using provided name and count parameter.
     *
     * @param name Name of the latch.
     * @param cnt Count for new latch creation. Ignored if {@code create} flag is {@code false}.
     * @param autoDel {@code True} to automatically delete latch from cache when its count reaches zero.
     *        Ignored if {@code create} flag is {@code false}.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Count down latch for the given name.
     * @throws IgniteException If latch could not be fetched or created.
     */
    public IgniteCountDownLatch countDownLatch(String name, int cnt, boolean autoDel, boolean create)
        throws IgniteException;

    /**
     * Gets or creates semaphore. If semaphore is not found in cache and {@code create} flag
     * is {@code true}, it is created using provided name and count parameter.
     *
     * @param name Name of the semaphore.
     * @param cnt Count for new semaphore creation. Ignored if {@code create} flag is {@code false}.
     * @param failoverSafe {@code True} to create failover safe semaphore which means that
     *      if any node leaves topology permits already acquired by that node are silently released
     *      and become available for alive nodes to acquire. If flag is {@code false} then
     *      all threads waiting for available permits get interrupted.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return Semaphore for the given name.
     * @throws IgniteException If semaphore could not be fetched or created.
     */
    public IgniteSemaphore semaphore(String name, int cnt, boolean failoverSafe, boolean create)
        throws IgniteException;

    /**
     * Gets or creates reentrant lock. If reentrant lock is not found in cache and {@code create} flag
     * is {@code true}, it is created using provided name.
     *
     * @param name Name of the lock.
     * @param failoverSafe {@code True} to create failover safe lock which means that
     *      if any node leaves topology, all locks already acquired by that node are silently released
     *      and become available for other nodes to acquire. If flag is {@code false} then
     *      all threads on other nodes waiting to acquire lock are interrupted.
     * @param fair If {@code True}, fair lock will be created.
     * @param create Boolean flag indicating whether data structure should be created if does not exist.
     * @return ReentrantLock for the given name.
     * @throws IgniteException If reentrant lock could not be fetched or created.
     */
    public IgniteLock reentrantLock(String name, boolean failoverSafe, boolean fair, boolean create)
        throws IgniteException;

    /**
     * Will get a named queue from cache and create one if it has not been created yet and {@code cfg} is not
     * {@code null}.
     * If queue is present already, queue properties will not be changed. Use
     * collocation for {@link CacheMode#PARTITIONED} caches if you have lots of relatively
     * small queues as it will make fetching, querying, and iteration a lot faster. If you have
     * few very large queues, then you should consider turning off collocation as they simply
     * may not fit in a single node's memory.
     *
     * @param name Name of queue.
     * @param cap Capacity of queue, {@code 0} for unbounded queue. Ignored if {@code cfg} is {@code null}.
     * @param cfg Queue configuration if new queue should be created.
     * @return Queue with given properties.
     * @throws IgniteException If queue could not be fetched or created.
     */
    public <T> IgniteQueue<T> queue(String name, int cap, @Nullable CollectionConfiguration cfg)
        throws IgniteException;

    /**
     * Will get a named set from cache and create one if it has not been created yet and {@code cfg} is not
     * {@code null}.
     *
     * @param name Set name.
     * @param cfg Set configuration if new set should be created.
     * @return Set with given properties.
     * @throws IgniteException If set could not be fetched or created.
     */
    public <T> IgniteSet<T> set(String name, @Nullable CollectionConfiguration cfg) throws IgniteException;

    /**
     * Gets an instance of deployed Ignite plugin.
     *
     * @param name Plugin name.
     * @param <T> Plugin type.
     * @return Plugin instance.
     * @throws PluginNotFoundException If plugin for the given name was not found.
     */
    public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException;

    /**
     * Gets an instance of {@link IgniteBinary} interface.
     *
     * @return Instance of {@link IgniteBinary} interface.
     */
    public IgniteBinary binary();

    /**
     * Closes {@code this} instance of grid. This method is identical to calling
     * {@link G#stop(String, boolean) G.stop(igniteInstanceName, true)}.
     * <p>
     * The method is invoked automatically on objects managed by the
     * {@code try-with-resources} statement.
     *
     * @throws IgniteException If failed to stop grid.
     */
    @Override public void close() throws IgniteException;

    /**
     * Gets affinity service to provide information about data partitioning and distribution.
     *
     * @param cacheName Cache name.
     * @param <K> Cache key type.
     * @return Affinity.
     */
    public <K> Affinity<K> affinity(String cacheName);

    /**
     * Checks Ignite grid is active or not active.
     *
     * @return {@code True} if grid is active. {@code False} If grid is not active.
     */
    public boolean active();

    /**
     * Changes Ignite grid state to active or inactive.
     *
     * @param active If {@code True} start activation process. If {@code False} start deactivation process.
     */
    public void active(boolean active);

    /**
     * Clears partition's lost state and moves caches to a normal mode.
     */
    public void resetLostPartitions(Collection<String> cacheNames);

    /**
     * Returns a collection of {@link MemoryMetrics} that reflects page memory usage on this Apache Ignite node
     * instance.
     * Returns the collection that contains the latest snapshots for each memory region
     * configured with {@link MemoryPolicyConfiguration configuration} on this Ignite node instance.
     *
     * @return Collection of {@link MemoryMetrics} snapshots.
     */
    public Collection<MemoryMetrics> memoryMetrics();

    /**
     * Returns the latest {@link MemoryMetrics} snapshot for the memory region of the given name.
     *
     * To get the metrics for the default memory region use
     * {@link MemoryConfiguration#DFLT_MEM_PLC_DEFAULT_NAME} as the name
     * or a custom name if the default memory region has been renamed.
     *
     * @param memPlcName Name of memory region configured with {@link MemoryPolicyConfiguration config}.
     * @return {@link MemoryMetrics} snapshot or {@code null} if no memory region is configured under specified name.
     */
    @Nullable public MemoryMetrics memoryMetrics(String memPlcName);

    /**
     *
     * @return {@link PersistenceMetrics} snapshot.
     */
    public PersistenceMetrics persistentStoreMetrics();
}
