/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.plugin.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.dr.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.messaging.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.scheduler.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.service.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.transactions.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Main entry-point for all GridGain APIs.
 * You can obtain an instance of {@code Grid} through {@link Ignition#grid()},
 * or for named grids you can use {@link Ignition#grid(String)}. Note that you
 * can have multiple instances of {@code Grid} running in the same VM by giving
 * each instance a different name.
 * <p>
 * Note that {@code Grid} extends {@link org.apache.ignite.cluster.ClusterGroup} which means that it provides grid projection
 * functionality over the whole grid (instead os a subgroup of nodes).
 * <p>
 * In addition to {@link org.apache.ignite.cluster.ClusterGroup} functionality, from here you can get the following:
 * <ul>
 * <li>{@link GridCache} - functionality for in-memory distributed cache.</li>
 * <li>{@link IgniteDataLoader} - functionality for loading data large amounts of data into cache.</li>
 * <li>{@link GridDr} - functionality for WAN-based Data Center Replication of in-memory cache.</li>
 * <li>{@link GridGgfs} - functionality for distributed Hadoop-compliant in-memory file system and map-reduce.</li>
 * <li>{@link GridStreamer} - functionality for streaming events workflow with queries and indexes into rolling windows.</li>
 * <li>{@link GridScheduler} - functionality for scheduling jobs using UNIX Cron syntax.</li>
 * <li>{@link GridProduct} - functionality for licence management and update and product related information.</li>
 * <li>{@link IgniteCompute} - functionality for executing tasks and closures on all grid nodes (inherited form {@link org.apache.ignite.cluster.ClusterGroup}).</li>
 * <li>{@link GridMessaging} - functionality for topic-based message exchange on all grid nodes (inherited form {@link org.apache.ignite.cluster.ClusterGroup}).</li>
 * <li>{@link GridEvents} - functionality for querying and listening to events on all grid nodes  (inherited form {@link org.apache.ignite.cluster.ClusterGroup}).</li>
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
    public GridLogger log();

    /**
     * Gets the configuration of this grid instance.
     * <p>
     * <b>NOTE:</b>
     * <br>
     * SPIs obtains through this method should never be used directly. SPIs provide
     * internal view on the subsystem and is used internally by GridGain kernal. In rare use cases when
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
     * @param prj Projection.
     * @return Compute instance over given projection.
     */
    public IgniteCompute compute(ClusterGroup prj);

    /**
     * Gets {@code messaging} functionality over this grid projection. All operations
     * on the returned {@link GridMessaging} instance will only include nodes from
     * this projection.
     *
     * @return Messaging instance over this grid projection.
     */
    public GridMessaging message();

    /**
     * @param prj Projection.
     * @return Messaging instance over given projection.
     */
    public GridMessaging message(ClusterGroup prj);

    /**
     * Gets {@code events} functionality over this grid projection. All operations
     * on the returned {@link GridEvents} instance will only include nodes from
     * this projection.
     *
     * @return Events instance over this grid projection.
     */
    public GridEvents events();

    /**
     * @param prj Projection.
     * @return Events instance over given projection.
     */
    public GridEvents events(ClusterGroup prj);

    /**
     * Gets {@code services} functionality over this grid projection. All operations
     * on the returned {@link GridMessaging} instance will only include nodes from
     * this projection.
     *
     * @return Services instance over this grid projection.
     */
    public GridServices services();

    /**
     * @param prj Projection.
     * @return {@code Services} functionality over given projection.
     */
    public GridServices services(ClusterGroup prj);

    /**
     * Creates new {@link ExecutorService} which will execute all submitted
     * {@link java.util.concurrent.Callable} and {@link Runnable} jobs on nodes in this grid projection.
     * This essentially
     * creates a <b><i>Distributed Thread Pool</i</b> that can be used as a
     * replacement for local thread pools.
     *
     * @return Grid-enabled {@code ExecutorService}.
     */
    public ExecutorService executorService();

    /**
     * @param prj Projection.
     * @return {@link ExecutorService} which will execute jobs on nodes in given projection.
     */
    public ExecutorService executorService(ClusterGroup prj);

    /**
     * Gets information about product as well as license management capabilities.
     *
     * @return Instance of product.
     */
    public GridProduct product();

    /**
     * Gets an instance of cron-based scheduler.
     *
     * @return Instance of scheduler.
     */
    public GridScheduler scheduler();

    /**
     * Gets an instance of {@code GridSecurity} interface. Available in enterprise edition only.
     *
     * @return Instance of {@code GridSecurity} interface.
     */
    public GridSecurity security();

    /**
     * Gets an instance of {@code GridPortables} interface. Available in enterprise edition only.
     *
     * @return Instance of {@code GridPortables} interface.
     */
    public GridPortables portables();

    /**
     * Gets an instance of Data Center Replication.
     *
     * @return Instance of Data Center Replication.
     */
    public GridDr dr();

    /**
     * Gets the cache instance for the given name, if one does not
     * exist {@link IllegalArgumentException} will be thrown.
     * Note that in case named cache instance is used as GGFS data or meta cache, {@link IllegalStateException}
     * will be thrown.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     * @param name Cache name.
     * @return Cache instance for given name.
     * @see GridGgfsConfiguration
     * @see GridGgfsConfiguration#getDataCacheName()
     * @see GridGgfsConfiguration#getMetaCacheName()
     */
    public <K, V> GridCache<K, V> cache(@Nullable String name);

    /**
     * Gets all configured caches.
     * Caches that are used as GGFS meta and data caches will not be returned in resulting collection.
     *
     * @see GridGgfsConfiguration
     * @see GridGgfsConfiguration#getDataCacheName()
     * @see GridGgfsConfiguration#getMetaCacheName()
     * @return All configured caches.
     */
    public Collection<GridCache<?, ?>> caches();

    /**
     * Gets grid transactions facade.
     *
     * @return Grid transactions facade.
     */
    public GridTransactions transactions();

    /**
     * Gets a new instance of data loader associated with given cache name. Data loader
     * is responsible for loading external data into in-memory data grid. For more information
     * refer to {@link IgniteDataLoader} documentation.
     *
     * @param cacheName Cache name ({@code null} for default cache).
     * @return Data loader.
     */
    public <K, V> IgniteDataLoader<K, V> dataLoader(@Nullable String cacheName);

    /**
     * Gets an instance of GGFS - GridGain In-Memory File System, if one is not
     * configured then {@link IllegalArgumentException} will be thrown.
     * <p>
     * GGFS is fully compliant with Hadoop {@code FileSystem} APIs and can
     * be plugged into Hadoop installations. For more information refer to
     * documentation on Hadoop integration shipped with GridGain.
     *
     * @param name GGFS name.
     * @return GGFS instance.
     */
    public GridGgfs ggfs(String name);

    /**
     * Gets all instances of the grid file systems.
     *
     * @return Collection of grid file systems instances.
     */
    public Collection<GridGgfs> ggfss();

    /**
     * Gets an instance of Hadoop.
     *
     * @return Hadoop instance.
     */
    public GridHadoop hadoop();

    /**
     * Gets an instance of streamer by name, if one does not exist then
     * {@link IllegalArgumentException} will be thrown.
     *
     * @param name Streamer name.
     * @return Streamer for given name.
     */
    public GridStreamer streamer(@Nullable String name);

    /**
     * Gets all instances of streamers.
     *
     * @return Collection of all streamer instances.
     */
    public Collection<GridStreamer> streamers();

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
     * @throws GridException If failed to stop grid.
     */
    @Override public void close() throws GridException;
}
