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
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Main entry-point for all Ignite APIs.
 * You can obtain an instance of {@code Grid} through {@link Ignition#ignite()},
 * or for named grids you can use {@link Ignition#ignite(String)}. Note that you
 * can have multiple instances of {@code Grid} running in the same VM by giving
 * each instance a different name.
 * <p>
 * Note that {@code Grid} extends {@link ClusterGroup} which means that it provides grid projection
 * functionality over the whole grid (instead of a subgroup of nodes).
 * <p>
 * In addition to {@link ClusterGroup} functionality, from here you can get the following:
 * <ul>
 * <li>{@link IgniteCache} - functionality for in-memory distributed cache.</li>
 * <li>{@link IgniteDataStreamer} - functionality for loading data large amounts of data into cache.</li>
 * <li>{@link IgniteFileSystem} - functionality for distributed Hadoop-compliant in-memory file system and map-reduce.</li>
 * <li>{@link IgniteStreamer} - functionality for streaming events workflow with queries and indexes into rolling windows.</li>
 * <li>{@link IgniteScheduler} - functionality for scheduling jobs using UNIX Cron syntax.</li>
 * <li>{@link IgniteCompute} - functionality for executing tasks and closures on all grid nodes (inherited form {@link ClusterGroup}).</li>
 * <li>{@link IgniteMessaging} - functionality for topic-based message exchange on all grid nodes (inherited form {@link ClusterGroup}).</li>
 * <li>{@link IgniteEvents} - functionality for querying and listening to events on all grid nodes  (inherited form {@link ClusterGroup}).</li>
 * </ul>
 */
public interface Ignite extends AutoCloseable {
    /**
     * Gets the name of the grid this grid instance (and correspondingly its local node) belongs to.
     * Note that single Java VM can have multiple grid instances all belonging to different grids. Grid
     * name allows to indicate to what grid this particular grid instance (i.e. grid runtime and its
     * local node) belongs to.
     * <p>
     * If default grid instance is used, then
     * {@code null} is returned. Refer to {@link Ignition} documentation
     * for information on how to start named grids.
     *
     * @return Name of the grid, or {@code null} for default grid.
     */
    public String name();

    /**
     * Gets grid's logger.
     *
     * @return Grid's logger.
     */
    public IgniteLogger log();

    /**
     * Gets the configuration of this grid instance.
     * <p>
     * <b>NOTE:</b>
     * <br>
     * SPIs obtains through this method should never be used directly. SPIs provide
     * internal view on the subsystem and is used internally by Ignite kernal. In rare use cases when
     * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
     * via this method to check its configuration properties or call other non-SPI
     * methods.
     *
     * @return Grid configuration instance.
     */
    public IgniteConfiguration configuration();

    /**
     * Gets an instance of {@link IgniteCluster} interface.
     *
     * @return Instance of {@link IgniteCluster} interface.
     */
    public IgniteCluster cluster();

    /**
     * Gets {@code compute} functionality over this grid projection. All operations
     * on the returned {@link IgniteCompute} instance will only include nodes from
     * this projection.
     *
     * @return Compute instance over this grid projection.
     */
    public IgniteCompute compute();

    /**
     * @param grp Cluster group..
     * @return Compute instance over given cluster group.
     */
    public IgniteCompute compute(ClusterGroup grp);

    /**
     * Gets {@code messaging} functionality over this grid projection. All operations
     * on the returned {@link IgniteMessaging} instance will only include nodes from
     * this projection.
     *
     * @return Messaging instance over this grid projection.
     */
    public IgniteMessaging message();

    /**
     * @param grp Cluster group.
     * @return Messaging instance over given cluster group.
     */
    public IgniteMessaging message(ClusterGroup grp);

    /**
     * Gets {@code events} functionality over this grid projection. All operations
     * on the returned {@link IgniteEvents} instance will only include nodes from
     * this projection.
     *
     * @return Events instance over this grid projection.
     */
    public IgniteEvents events();

    /**
     * @param grp Cluster group.
     * @return Events instance over given cluster group.
     */
    public IgniteEvents events(ClusterGroup grp);

    /**
     * Gets {@code services} functionality over this grid projection. All operations
     * on the returned {@link IgniteMessaging} instance will only include nodes from
     * this projection.
     *
     * @return Services instance over this grid projection.
     */
    public IgniteServices services();

    /**
     * @param grp Cluster group.
     * @return {@code Services} functionality over given cluster group.
     */
    public IgniteServices services(ClusterGroup grp);

    /**
     * Creates new {@link ExecutorService} which will execute all submitted
     * {@link java.util.concurrent.Callable} and {@link Runnable} jobs on nodes in this grid projection.
     * This essentially
     * creates a <b><i>Distributed Thread Pool</i></b> that can be used as a
     * replacement for local thread pools.
     *
     * @return Grid-enabled {@code ExecutorService}.
     */
    public ExecutorService executorService();

    /**
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
     * Gets an instance of {@link IgniteCache} API. {@code IgniteCache} is a fully-compatible
     * implementation of {@code JCache (JSR 107)} specification.
     *
     * @param name Cache name.
     * @return Instance of the cache for the specified name.
     */
    public <K, V> IgniteCache<K, V> jcache(@Nullable String name);

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
     * @param cacheName Cache name ({@code null} for default cache).
     * @return Data streamer.
     */
    public <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String cacheName);

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
    public IgniteFileSystem fileSystem(String name);

    /**
     * Gets all instances of IGFS (Ignite In-Memory File System).
     *
     * @return Collection of IGFS instances.
     */
    public Collection<IgniteFileSystem> fileSystems();

    /**
     * Gets an instance of streamer by name, if one does not exist then
     * {@link IllegalArgumentException} will be thrown.
     *
     * @param name Streamer name.
     * @return Streamer for given name.
     */
    public IgniteStreamer streamer(@Nullable String name);

    /**
     * Gets all instances of streamers.
     *
     * @return Collection of all streamer instances.
     */
    public Collection<IgniteStreamer> streamers();

    /**
     * Will get an atomic sequence from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
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
     * Will get a atomic reference from cache and create one if it has not been created yet and {@code create} flag
     * is {@code true}.
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
     * Closes {@code this} instance of grid. This method is identical to calling
     * {@link G#stop(String, boolean) G.stop(gridName, true)}.
     * <p>
     * The method is invoked automatically on objects managed by the
     * {@code try-with-resources} statement.
     *
     * @throws IgniteException If failed to stop grid.
     */
    @Override public void close() throws IgniteException;

    /**
     * Gets affinity service to provide information about data partitioning
     * and distribution.
     * @param cacheName Cache name.
     * @param <K> Cache key type.
     * @return Affinity.
     */
    public <K> CacheAffinity<K> affinity(String cacheName);
}
