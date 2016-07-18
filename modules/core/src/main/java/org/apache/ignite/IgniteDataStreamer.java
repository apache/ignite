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
import java.util.Map;
import javax.cache.CacheException;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.stream.StreamReceiver;
import org.jetbrains.annotations.Nullable;

/**
 * Data streamer is responsible for streaming external data into cache. It achieves it by
 * properly buffering updates and properly mapping keys to nodes responsible for the data
 * to make sure that there is the least amount of data movement possible and optimal
 * network and memory utilization.
 * <p>
 * Note that streamer will stream data concurrently by multiple internal threads, so the
 * data may get to remote nodes in different order from which it was added to
 * the streamer.
 * <p>
 * Also note that {@code IgniteDataStreamer} is not the only way to add data into cache.
 * Alternatively you can use {@link IgniteCache#loadCache(IgniteBiPredicate, Object...)}
 * method to add data from underlying data store. You can also use standard
 * cache {@code put(...)} and {@code putAll(...)} operations as well, but they most
 * likely will not perform as well as this class for adding data. And finally,
 * data can be added from underlying data store on demand, whenever it is accessed -
 * for this no explicit data adding step is needed.
 * <p>
 * {@code IgniteDataStreamer} supports the following configuration properties:
 * <ul>
 *  <li>
 *      {@link #perNodeBufferSize(int)} - when entries are added to data streamer via
 *      {@link #addData(Object, Object)} method, they are not sent to in-memory data grid right
 *      away and are buffered internally for better performance and network utilization.
 *      This setting controls the size of internal per-node buffer before buffered data
 *      is sent to remote node. Default is defined by {@link #DFLT_PER_NODE_BUFFER_SIZE}
 *      value.
 *  </li>
 *  <li>
 *      {@link #perNodeParallelOperations(int)} - sometimes data may be added
 *      to the data streamer via {@link #addData(Object, Object)} method faster than it can
 *      be put in cache. In this case, new buffered stream messages are sent to remote nodes
 *      before responses from previous ones are received. This could cause unlimited heap
 *      memory utilization growth on local and remote nodes. To control memory utilization,
 *      this setting limits maximum allowed number of parallel buffered stream messages that
 *      are being processed on remote nodes. If this number is exceeded, then
 *      {@link #addData(Object, Object)} method will block to control memory utilization.
 *      Default is defined by {@link #DFLT_MAX_PARALLEL_OPS} value.
 *  </li>
 *  <li>
 *      {@link #autoFlushFrequency(long)} - automatic flush frequency in milliseconds. Essentially,
 *      this is the time after which the streamer will make an attempt to submit all data
 *      added so far to remote nodes. Note that there is no guarantee that data will be
 *      delivered after this concrete attempt (e.g., it can fail when topology is
 *      changing), but it won't be lost anyway. Disabled by default (default value is {@code 0}).
 *  </li>
 *  <li>
 *      {@link #allowOverwrite(boolean)} - Sets flag enabling overwriting existing values in cache.
 *      Data streamer will perform better if this flag is disabled, which is the default setting.
 *  </li>
 *  <li>
 *      {@link #receiver(StreamReceiver)} - defines how cache will be updated with added entries.
 *      It allows to provide user-defined custom logic to update the cache in the most effective and flexible way.
 *  </li>
 *  <li>
 *      {@link #deployClass(Class)} - optional deploy class for peer deployment. All classes
 *      streamed by a data streamer must be class-loadable from the same class-loader.
 *      Ignite will make the best effort to detect the most suitable class-loader
 *      for data loading. However, in complex cases, where compound or deeply nested
 *      class-loaders are used, it is best to specify a deploy class which can be any
 *      class loaded by the class-loader for given data.
 *  </li>
 * </ul>
 */
public interface IgniteDataStreamer<K, V> extends AutoCloseable {
    /** Default max concurrent put operations count. */
    public static final int DFLT_MAX_PARALLEL_OPS = 16;

    /** Default per node buffer size. */
    public static final int DFLT_PER_NODE_BUFFER_SIZE = 1024;

    /** Default timeout for streamer's operations. */
    public static final long DFLT_UNLIMIT_TIMEOUT = -1;

    /**
     * Name of cache to stream data to.
     *
     * @return Cache name or {@code null} for default cache.
     */
    public String cacheName();

    /**
     * Gets flag enabling overwriting existing values in cache.
     * Data streamer will perform better if this flag is disabled.
     * <p>
     * This flag is disabled by default (default is {@code false}).
     *
     * @return {@code True} if overwriting is allowed, {@code false} otherwise..
     */
    public boolean allowOverwrite();

    /**
     * Sets flag enabling overwriting existing values in cache.
     * Data streamer will perform better if this flag is disabled.
     * <p>
     * Should not be used when custom cache receiver set using {@link #receiver(StreamReceiver)} method.
     * <p>
     * Note that when this flag is {@code false}, updates will not be propagated to the cache store
     * (i.e. {@link #skipStore()} flag will be set to {@code true} implicitly).
     * <p>
     * This flag is disabled by default (default is {@code false}).
     *
     * @param allowOverwrite Flag value.
     * @throws CacheException If failed.
     */
    public void allowOverwrite(boolean allowOverwrite) throws CacheException;

    /**
     * Gets flag indicating that write-through behavior should be disabled for data streaming.
     * Default is {@code false}.
     *
     * @return Skip store flag.
     */
    public boolean skipStore();

    /**
     * Sets flag indicating that write-through behavior should be disabled for data streaming.
     * Default is {@code false}.
     *
     * @param skipStore Skip store flag.
     */
    public void skipStore(boolean skipStore);

    /**
     * Gets flag indicating that objects should be kept in binary format when passed to the stream receiver.
     * Default is {@code false}.
     *
     * @return Skip store flag.
     */
    public boolean keepBinary();

    /**
     * Sets flag indicating that objects should be kept in binary format when passes to the steam receiver.
     * Default is {@code false}.
     *
     * @param keepBinary Keep binary flag.
     */
    public void keepBinary(boolean keepBinary);

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
     * Gets maximum number of parallel stream operations for a single node.
     *
     * @return Maximum number of parallel stream operations for a single node.
     */
    public int perNodeParallelOperations();

    /**
     * Sets maximum number of parallel stream operations for a single node.
     * <p>
     * This method should be called prior to {@link #addData(Object, Object)} call.
     * <p>
     * If not provided, default value is {@link #DFLT_MAX_PARALLEL_OPS}.
     *
     * @param parallelOps Maximum number of parallel stream operations for a single node.
     */
    public void perNodeParallelOperations(int parallelOps);

    /**
     * Sets the timeout that is used in the following cases:
     * <ul>
     * <li>any data addition method can be blocked when all per node parallel operations are exhausted.
     * The timeout defines the max time you will be blocked waiting for a permit to add a chunk of data
     * into the streamer;</li>
     * <li>Total timeout time for {@link #flush()} operation;</li>
     * <li>Total timeout time for {@link #close()} operation.</li>
     * </ul>
     * By default the timeout is disabled.
     *
     * @param timeout Timeout in milliseconds.
     * @throws IllegalArgumentException If timeout is zero or less than {@code -1}.
     */
    public void timeout(long timeout);

    /**
     * Gets timeout set by {@link #timeout(long)}.
     *
     * @return Timeout in milliseconds.
     */
    public long timeout();

    /**
     * Gets automatic flush frequency. Essentially, this is the time after which the
     * streamer will make an attempt to submit all data added so far to remote nodes.
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
     * streamer will make an attempt to submit all data added so far to remote nodes.
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
     * Gets future for this streaming process. This future completes whenever method
     * {@link #close(boolean)} completes. By attaching listeners to this future
     * it is possible to get asynchronous notifications for completion of this
     * streaming process.
     *
     * @return Future for this streaming process.
     */
    public IgniteFuture<?> future();

    /**
     * Optional deploy class for peer deployment. All classes added by a data streamer
     * must be class-loadable from the same class-loader. Ignite will make the best
     * effort to detect the most suitable class-loader for data loading. However,
     * in complex cases, where compound or deeply nested class-loaders are used,
     * it is best to specify a deploy class which can be any class loaded by
     * the class-loader for given data.
     *
     * @param depCls Any class loaded by the class-loader for given data.
     */
    public void deployClass(Class<?> depCls);

    /**
     * Sets custom stream receiver to this data streamer.
     *
     * @param rcvr Stream receiver.
     */
    public void receiver(StreamReceiver<K, V> rcvr);

    /**
     * Adds key for removal on remote node. Equivalent to {@link #addData(Object, Object) addData(key, null)}.
     *
     * @param key Key.
     * @return Future fo this operation.
     * @throws CacheException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on streamer.
     */
    public IgniteFuture<?> removeData(K key)  throws CacheException, IgniteInterruptedException, IllegalStateException;

    /**
     * Adds data for streaming on remote node. This method can be called from multiple
     * threads in parallel to speed up streaming if needed.
     * <p>
     * Note that streamer will stream data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the streamer.
     * <p>
     * Note: if {@link IgniteDataStreamer#allowOverwrite()} set to {@code false} (by default)
     * then data streamer will not overwrite existing cache entries for better performance
     * (to change, set {@link IgniteDataStreamer#allowOverwrite(boolean)} to {@code true})
     *
     * @param key Key.
     * @param val Value or {@code null} if respective entry must be removed from cache.
     * @return Future fo this operation.
     * @throws CacheException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on streamer.
     * @throws IgniteDataStreamerTimeoutException If {@code timeout} is exceeded.
     * @see #allowOverwrite()
     */
    public IgniteFuture<?> addData(K key, @Nullable V val) throws CacheException, IgniteInterruptedException,
        IllegalStateException, IgniteDataStreamerTimeoutException;

    /**
     * Adds data for streaming on remote node. This method can be called from multiple
     * threads in parallel to speed up streaming if needed.
     * <p>
     * Note that streamer will stream data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the streamer.
     * <p>
     * Note: if {@link IgniteDataStreamer#allowOverwrite()} set to {@code false} (by default)
     * then data streamer will not overwrite existing cache entries for better performance
     * (to change, set {@link IgniteDataStreamer#allowOverwrite(boolean)} to {@code true})
     *
     * @param entry Entry.
     * @return Future fo this operation.
     * @throws CacheException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on streamer.
     * @throws IgniteDataStreamerTimeoutException If {@code timeout} is exceeded.
     * @see #allowOverwrite()
     */
    public IgniteFuture<?> addData(Map.Entry<K, V> entry) throws CacheException, IgniteInterruptedException,
        IllegalStateException, IgniteDataStreamerTimeoutException;

    /**
     * Adds data for streaming on remote node. This method can be called from multiple
     * threads in parallel to speed up streaming if needed.
     * <p>
     * Note that streamer will stream data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the streamer.
     * <p>
     * Note: if {@link IgniteDataStreamer#allowOverwrite()} set to {@code false} (by default)
     * then data streamer will not overwrite existing cache entries for better performance
     * (to change, set {@link IgniteDataStreamer#allowOverwrite(boolean)} to {@code true})
     *
     * @param entries Collection of entries to be streamed.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on streamer.
     * @throws IgniteDataStreamerTimeoutException If {@code timeout} is exceeded.
     * @return Future for this stream operation.
     * @see #allowOverwrite()
     */
    public IgniteFuture<?> addData(Collection<? extends Map.Entry<K, V>> entries) throws IllegalStateException,
        IgniteDataStreamerTimeoutException;

    /**
     * Adds data for streaming on remote node. This method can be called from multiple
     * threads in parallel to speed up streaming if needed.
     * <p>
     * Note that streamer will stream data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the streamer.
     * <p>
     * Note: if {@link IgniteDataStreamer#allowOverwrite()} set to {@code false} (by default)
     * then data streamer will not overwrite existing cache entries for better performance
     * (to change, set {@link IgniteDataStreamer#allowOverwrite(boolean)} to {@code true})
     *
     * @param entries Map to be streamed.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on streamer.
     * @throws IgniteDataStreamerTimeoutException If {@code timeout} is exceeded.
     * @return Future for this stream operation.
     * @see #allowOverwrite()
     */
    public IgniteFuture<?> addData(Map<K, V> entries) throws IllegalStateException,
        IgniteDataStreamerTimeoutException;

    /**
     * Streams any remaining data, but doesn't close the streamer. Data can be still added after
     * flush is finished. This method blocks and doesn't allow to add any data until all data
     * is streamed.
     * <p>
     * If another thread is already performing flush, this method will block, wait for
     * another thread to complete flush and exit. If you don't want to wait in this case,
     * use {@link #tryFlush()} method.
     *
     * @throws CacheException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on streamer.
     * @throws IgniteDataStreamerTimeoutException If {@code timeout} is exceeded.
     * @see #tryFlush()
     */
    public void flush() throws CacheException, IgniteInterruptedException, IllegalStateException,
        IgniteDataStreamerTimeoutException;

    /**
     * Makes an attempt to stream remaining data. This method is mostly similar to {@link #flush},
     * with the difference that it won't wait and will exit immediately.
     *
     * @throws CacheException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on streamer.
     * @see #flush()
     */
    public void tryFlush() throws CacheException, IgniteInterruptedException, IllegalStateException;

    /**
     * Streams any remaining data and closes this streamer.
     *
     * @param cancel {@code True} to cancel ongoing streaming operations.
     * @throws CacheException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IgniteDataStreamerTimeoutException If {@code timeout} is exceeded, only if cancel is {@code false}.
     */
    public void close(boolean cancel) throws CacheException, IgniteInterruptedException,
        IgniteDataStreamerTimeoutException;

    /**
     * Closes data streamer. This method is identical to calling {@link #close(boolean) close(false)} method.
     * <p>
     * The method is invoked automatically on objects managed by the
     * {@code try-with-resources} statement.
     *
     * @throws CacheException If failed to close data streamer.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IgniteDataStreamerTimeoutException If {@code timeout} is exceeded.
     */
    @Override public void close() throws CacheException, IgniteInterruptedException,
        IgniteDataStreamerTimeoutException;

}