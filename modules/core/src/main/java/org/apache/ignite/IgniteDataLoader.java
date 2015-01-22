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

import org.apache.ignite.dataload.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Data loader is responsible for loading external data into cache. It achieves it by
 * properly buffering updates and properly mapping keys to nodes responsible for the data
 * to make sure that there is the least amount of data movement possible and optimal
 * network and memory utilization.
 * <p>
 * Note that loader will load data concurrently by multiple internal threads, so the
 * data may get to remote nodes in different order from which it was added to
 * the loader.
 * <p>
 * Also note that {@code GridDataLoader} is not the only way to load data into cache.
 * Alternatively you can use {@link org.apache.ignite.cache.GridCache#loadCache(org.apache.ignite.lang.IgniteBiPredicate, long, Object...)}
 * method to load data from underlying data store. You can also use standard
 * cache {@code put(...)} and {@code putAll(...)} operations as well, but they most
 * likely will not perform as well as this class for loading data. And finally,
 * data can be loaded from underlying data store on demand, whenever it is accessed -
 * for this no explicit data loading step is needed.
 * <p>
 * {@code GridDataLoader} supports the following configuration properties:
 * <ul>
 *  <li>
 *      {@link #perNodeBufferSize(int)} - when entries are added to data loader via
 *      {@link #addData(Object, Object)} method, they are not sent to in-memory data grid right
 *      away and are buffered internally for better performance and network utilization.
 *      This setting controls the size of internal per-node buffer before buffered data
 *      is sent to remote node. Default is defined by {@link #DFLT_PER_NODE_BUFFER_SIZE}
 *      value.
 *  </li>
 *  <li>
 *      {@link #perNodeParallelLoadOperations(int)} - sometimes data may be added
 *      to the data loader via {@link #addData(Object, Object)} method faster than it can
 *      be put in cache. In this case, new buffered load messages are sent to remote nodes
 *      before responses from previous ones are received. This could cause unlimited heap
 *      memory utilization growth on local and remote nodes. To control memory utilization,
 *      this setting limits maximum allowed number of parallel buffered load messages that
 *      are being processed on remote nodes. If this number is exceeded, then
 *      {@link #addData(Object, Object)} method will block to control memory utilization.
 *      Default is defined by {@link #DFLT_MAX_PARALLEL_OPS} value.
 *  </li>
 *  <li>
 *      {@link #autoFlushFrequency(long)} - automatic flush frequency in milliseconds. Essentially,
 *      this is the time after which the loader will make an attempt to submit all data
 *      added so far to remote nodes. Note that there is no guarantee that data will be
 *      delivered after this concrete attempt (e.g., it can fail when topology is
 *      changing), but it won't be lost anyway. Disabled by default (default value is {@code 0}).
 *  </li>
 *  <li>
 *      {@link #isolated(boolean)} - defines if data loader will assume that there are no other concurrent
 *      updates and allow data loader choose most optimal concurrent implementation.
 *  </li>
 *  <li>
 *      {@link #updater(IgniteDataLoadCacheUpdater)} - defines how cache will be updated with loaded entries.
 *      It allows to provide user-defined custom logic to update the cache in the most effective and flexible way.
 *  </li>
 *  <li>
 *      {@link #deployClass(Class)} - optional deploy class for peer deployment. All classes
 *      loaded by a data loader must be class-loadable from the same class-loader.
 *      GridGain will make the best effort to detect the most suitable class-loader
 *      for data loading. However, in complex cases, where compound or deeply nested
 *      class-loaders are used, it is best to specify a deploy class which can be any
 *      class loaded by the class-loader for given data.
 *  </li>
 * </ul>
 */
public interface IgniteDataLoader<K, V> extends AutoCloseable {
    /** Default max concurrent put operations count. */
    public static final int DFLT_MAX_PARALLEL_OPS = 16;

    /** Default per node buffer size. */
    public static final int DFLT_PER_NODE_BUFFER_SIZE = 1024;

    /**
     * Name of cache to load data to.
     *
     * @return Cache name or {@code null} for default cache.
     */
    @Nullable public String cacheName();

    /**
     * Gets flag value indicating that this data loader assumes that there are no other concurrent updates to the cache.
     * Default is {@code false}.
     *
     * @return Flag value.
     */
    public boolean isolated();

    /**
     * Sets flag indicating that this data loader should assume that there are no other concurrent updates to the cache.
     * Should not be used when custom cache updater set using {@link #updater(IgniteDataLoadCacheUpdater)} method.
     * Default is {@code false}.
     *
     * @param isolated Flag value.
     * @throws IgniteCheckedException If failed.
     */
    public void isolated(boolean isolated) throws IgniteCheckedException;

    /**
     * Gets flag indicating that write-through behavior should be disabled for data loading.
     * Default is {@code false}.
     *
     * @return Skip store flag.
     */
    public boolean skipStore();

    /**
     * Sets flag indicating that write-through behavior should be disabled for data loading.
     * Default is {@code false}.
     *
     * @param skipStore Skip store flag.
     */
    public void skipStore(boolean skipStore);

    /**
     * Gets size of per node key-value pairs buffer.
     *
     * @return Per node buffer size.
     */
    public int perNodeBufferSize();

    /**
     * Sets size of per node key-value pairs buffer.
     * <p>
     * This method should be called prior to {@link #addData(Object, Object)} call.
     * <p>
     * If not provided, default value is {@link #DFLT_PER_NODE_BUFFER_SIZE}.
     *
     * @param bufSize Per node buffer size.
     */
    public void perNodeBufferSize(int bufSize);

    /**
     * Gets maximum number of parallel load operations for a single node.
     *
     * @return Maximum number of parallel load operations for a single node.
     */
    public int perNodeParallelLoadOperations();

    /**
     * Sets maximum number of parallel load operations for a single node.
     * <p>
     * This method should be called prior to {@link #addData(Object, Object)} call.
     * <p>
     * If not provided, default value is {@link #DFLT_MAX_PARALLEL_OPS}.
     *
     * @param parallelOps Maximum number of parallel load operations for a single node.
     */
    public void perNodeParallelLoadOperations(int parallelOps);

    /**
     * Gets automatic flush frequency. Essentially, this is the time after which the
     * loader will make an attempt to submit all data added so far to remote nodes.
     * Note that there is no guarantee that data will be delivered after this concrete
     * attempt (e.g., it can fail when topology is changing), but it won't be lost anyway.
     * <p>
     * If set to {@code 0}, automatic flush is disabled.
     * <p>
     * Automatic flush is disabled by default (default value is {@code 0}).
     *
     * @return Flush frequency or {@code 0} if automatic flush is disabled.
     * @see #flush()
     */
    public long autoFlushFrequency();

    /**
     * Sets automatic flush frequency. Essentially, this is the time after which the
     * loader will make an attempt to submit all data added so far to remote nodes.
     * Note that there is no guarantee that data will be delivered after this concrete
     * attempt (e.g., it can fail when topology is changing), but it won't be lost anyway.
     * <p>
     * If set to {@code 0}, automatic flush is disabled.
     * <p>
     * Automatic flush is disabled by default (default value is {@code 0}).
     *
     * @param autoFlushFreq Flush frequency or {@code 0} to disable automatic flush.
     * @see #flush()
     */
    public void autoFlushFrequency(long autoFlushFreq);

    /**
     * Gets future for this loading process. This future completes whenever method
     * {@link #close(boolean)} completes. By attaching listeners to this future
     * it is possible to get asynchronous notifications for completion of this
     * loading process.
     *
     * @return Future for this loading process.
     */
    public IgniteFuture<?> future();

    /**
     * Optional deploy class for peer deployment. All classes loaded by a data loader
     * must be class-loadable from the same class-loader. GridGain will make the best
     * effort to detect the most suitable class-loader for data loading. However,
     * in complex cases, where compound or deeply nested class-loaders are used,
     * it is best to specify a deploy class which can be any class loaded by
     * the class-loader for given data.
     *
     * @param depCls Any class loaded by the class-loader for given data.
     */
    public void deployClass(Class<?> depCls);

    /**
     * Sets custom cache updater to this data loader.
     *
     * @param updater Cache updater.
     */
    public void updater(IgniteDataLoadCacheUpdater<K, V> updater);

    /**
     * Adds key for removal on remote node. Equivalent to {@link #addData(Object, Object) addData(key, null)}.
     *
     * @param key Key.
     * @return Future fo this operation.
     * @throws IgniteCheckedException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on loader.
     */
    public IgniteFuture<?> removeData(K key)  throws IgniteCheckedException, IgniteInterruptedException, IllegalStateException;

    /**
     * Adds data for loading on remote node. This method can be called from multiple
     * threads in parallel to speed up loading if needed.
     * <p>
     * Note that loader will load data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the loader.
     *
     * @param key Key.
     * @param val Value or {@code null} if respective entry must be removed from cache.
     * @return Future fo this operation.
     * @throws IgniteCheckedException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on loader.
     */
    public IgniteFuture<?> addData(K key, @Nullable V val) throws IgniteCheckedException, IgniteInterruptedException,
        IllegalStateException;

    /**
     * Adds data for loading on remote node. This method can be called from multiple
     * threads in parallel to speed up loading if needed.
     * <p>
     * Note that loader will load data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the loader.
     *
     * @param entry Entry.
     * @return Future fo this operation.
     * @throws IgniteCheckedException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on loader.
     */
    public IgniteFuture<?> addData(Map.Entry<K, V> entry) throws IgniteCheckedException, IgniteInterruptedException,
        IllegalStateException;

    /**
     * Adds data for loading on remote node. This method can be called from multiple
     * threads in parallel to speed up loading if needed.
     * <p>
     * Note that loader will load data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the loader.
     *
     * @param entries Collection of entries to be loaded.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on loader.
     * @return Future for this load operation.
     */
    public IgniteFuture<?> addData(Collection<? extends Map.Entry<K, V>> entries) throws IllegalStateException;

    /**
     * Adds data for loading on remote node. This method can be called from multiple
     * threads in parallel to speed up loading if needed.
     * <p>
     * Note that loader will load data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the loader.
     *
     * @param entries Map to be loaded.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on loader.
     * @return Future for this load operation.
     */
    public IgniteFuture<?> addData(Map<K, V> entries) throws IllegalStateException;

    /**
     * Loads any remaining data, but doesn't close the loader. Data can be still added after
     * flush is finished. This method blocks and doesn't allow to add any data until all data
     * is loaded.
     * <p>
     * If another thread is already performing flush, this method will block, wait for
     * another thread to complete flush and exit. If you don't want to wait in this case,
     * use {@link #tryFlush()} method.
     *
     * @throws IgniteCheckedException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on loader.
     * @see #tryFlush()
     */
    public void flush() throws IgniteCheckedException, IgniteInterruptedException, IllegalStateException;

    /**
     * Makes an attempt to load remaining data. This method is mostly similar to {@link #flush},
     * with the difference that it won't wait and will exit immediately.
     *
     * @throws IgniteCheckedException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on loader.
     * @see #flush()
     */
    public void tryFlush() throws IgniteCheckedException, IgniteInterruptedException, IllegalStateException;

    /**
     * Loads any remaining data and closes this loader.
     *
     * @param cancel {@code True} to cancel ongoing loading operations.
     * @throws IgniteCheckedException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     */
    public void close(boolean cancel) throws IgniteCheckedException, IgniteInterruptedException;

    /**
     * Closes data loader. This method is identical to calling {@link #close(boolean) close(false)} method.
     * <p>
     * The method is invoked automatically on objects managed by the
     * {@code try-with-resources} statement.
     *
     * @throws IgniteCheckedException If failed to close data loader.
     * @throws IgniteInterruptedException If thread has been interrupted.
     */
    @Override public void close() throws IgniteCheckedException, IgniteInterruptedException;
}
