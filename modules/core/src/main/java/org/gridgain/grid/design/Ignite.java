// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design;

import org.gridgain.grid.design.cluster.*;
import org.gridgain.grid.design.plugin.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Main gateway for Apache Ignite functionality.
 *
 * @author @java.author
 * @version @java.version
 */
public interface Ignite {
    /**
     * Gets an instance of {@link IgniteCluster} interface.
     *
     * @return Instance of {@link IgniteCluster} interface.
     */
    public IgniteCluster cluster();

    /**
     * Gets an instance of {@link IgniteCompute} interface to compute over the whole cluster.
     *
     * @return Instance of {@link IgniteCluster} interface to compute over the whole cluster.
     */
    public IgniteCompute compute();

    /**
     * Gets an instance of {@link IgniteCompute} interface to compute over the specified cluster sub-group.
     *
     * @return Instance of {@link IgniteCluster} interface to compute over the specified cluster sub-group.
     */
    public IgniteCompute compute(ClusterGroup grp);

    /**
     * Gets an instance of standard Java {@link ExecutorService} API which will load-balance
     * jobs within the whole cluster.
     *
     * @return Grid-enabled {@link ExecutorService} API.
     */
    public ExecutorService executorService();

    /**
     * Gets an instance of standard Java {@link ExecutorService} API which will load-balance
     * jobs within the specified cluster sub-group.
     *
     * @return Grid-enabled {@link ExecutorService} API.
     */
    public ExecutorService executorService(ClusterGroup grp);

    /**
     * Gets an instance of {@link IgniteManaged} API which will deploy managed user services on
     * the whole cluster.
     *
     * @return Instance of {@link IgniteManaged} API.
     */
    public IgniteManaged managed();

    /**
     * Gets an instance of {@link IgniteManaged} API which will deploy managed user services on
     * the specified cluster sub-group.
     *
     * @return Instance of {@link IgniteManaged} API.
     */
    public IgniteManaged managed(ClusterGroup grp);

    /**
     * Gets an instance of {@link IgniteEvents} API which can subscribe for events within
     * the whole cluster.
     *
     * @return Instance of {@link IgniteEvents} API.
     */
    public IgniteEvents events();

    /**
     * Gets an instance of {@link IgniteEvents} API which can subscribe for events within
     * the specified cluster sub-group.
     *
     * @return Instance of {@link IgniteEvents} API.
     */
    public IgniteEvents events(ClusterGroup grp);

    /**
     * Gets an instance of {@link IgniteMessaging} API which can send messages within
     * the whole cluster.
     *
     * @return Instance of {@link IgniteMessaging} API.
     */
    public IgniteMessaging message();

    /**
     * Gets an instance of {@link IgniteMessaging} API which can send messages within
     * the specified cluster sub-group.
     *
     * @return Instance of {@link IgniteMessaging} API.
     */
    public IgniteMessaging message(ClusterGroup grp);

    /**
     * Gets an instance of {@link IgniteCache} API. {@code IgniteCache} is a fully-compatible
     * implementation of {@code JCache (JSR 107)} specification.
     *
     * @param name Cache name.
     * @return Instance of the cache for the specified name.
     */
    public <K, V> IgniteCache<K, V> cache(String name);

    /**
     * Gets a new instance of data loader associated with given cache name. Data loader
     * is responsible for loading external data into in-memory data grid. For more information
     * refer to {@link IgniteDataLoader} documentation.
     *
     * @param cacheName Cache name ({@code null} for default cache).
     * @return Data loader.
     */
    public <K, V> IgniteDataLoader<K, V> dataLoader(String cacheName);

    /**
     * Gets an instance of {@link IgniteQueue} API. {@code IgniteQueue} is a grid-enabled
     * implementation of the standard Java {@link BlockingDeque} API.
     *
     * @param name Queue name.
     * @return Instance of the grid-enabled blocking queue for the specified name.
     */
    public <T> IgniteQueue<T> queue(String name);

    /**
     * Gets an instance of {@link IgniteSet} API. {@code IgniteSet} is a grid-enabled
     * implementation of the standard Java {@link Set} API.
     *
     * @param name Set name.
     * @return Instance of the grid-enabled set for the specified name.
     */
    public <T> IgniteSet<? extends T> set(String name);

    /**
     * Gets an instance of {@link IgniteAtomicLong} API. {@code IgniteAtomicLong} is a grid-enabled
     * implementation of the standard Java {@link AtomicLong} API.
     *
     * @param name {@link IgniteAtomicLong} name.
     * @return Instance of the grid-enabled {@link AtomicLong} for the specified name.
     */
    public IgniteAtomicLong atomicLong(String name);

    /**
     * Gets an instance of {@link IgniteAtomicSequence} API. {@code IgniteAtomicSequence} is a grid-enabled
     * implementation of a sequence, which can be used for generating cluster-wide unique IDs.
     *
     * @param name {@link IgniteAtomicSequence} name.
     * @return Instance of the grid-enabled {@link IgniteAtomicSequence} for the specified name.
     */
    public IgniteAtomicSequence atomicSequence(String name);

    /**
     * Gets an instance of {@link IgniteAtomicReference} API. {@code IgniteAtomicReference} is a grid-enabled
     * implementation of the standard Java {@link AtomicReference} API.
     *
     * @param name {@link IgniteAtomicReference} name.
     * @return Instance of the grid-enabled {@link AtomicReference} for the specified name.
     */
    public <T> IgniteAtomicReference<T> atomicReference(String name);

    /**
     * Gets an instance of {@link IgniteAtomicStamped} API. {@code IgniteAtomicStamped} is a grid-enabled
     * implementation of the standard Java {@link AtomicStampedReference} API.
     *
     * @param name {@link IgniteAtomicStamped} name.
     * @return Instance of the grid-enabled {@link AtomicStampedReference} for the specified name.
     */
    public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name);

    /**
     * Gets an instance of {@link IgniteCountDownLatch} API. {@code IgniteCountDownLatch} is a grid-enabled
     * implementation of the standard Java {@link CountDownLatch} API.
     *
     * @param name {@link IgniteCountDownLatch} name.
     * @return Instance of the grid-enabled {@link CountDownLatch} for the specified name.
     */
    public IgniteCountDownLatch countDownLatch(String name);

    /**
     * Gets an instance of the {@link IgniteStreamer} for the specified name.
     *
     * @param name Name of the streamer.
     * @return Instance of the {@link IgniteStreamer} for the specified name.
     */
    public IgniteStreamer streamer(String name);

    /**
     * Gets an instance of the {@link IgniteFs} - {@code Ignite In-Memory File System} for the specified name.
     *
     * @param name Name of the Ignite in-memory file system.
     * @return Instance of the {@link IgniteFs} for the specified name.
     */
    public IgniteFs fileSystem(String name);

    /**
     * Gets an instance of the {@link IgniteScheduler} API which allows to schedule Ignite cluster-wide
     * computational tasks with {@code cron-like} syntax.
     *
     * @return Instance of the {@link IgniteScheduler} API.
     */
    public IgniteScheduler scheduler();

    /**
     * Gets an instance of deployed Ignite plugin.
     *
     * @param name Plugin name.
     * @param <T> Plugin type.
     * @return Plugin instance.
     * @throws PluginNotFoundException If plugin for the given name was not found.
     */
    public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException;
}
