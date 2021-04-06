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

package org.apache.ignite.client;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import javax.cache.CacheException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteDataStreamerTimeoutException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * TODO Javadoc, exceptions.
 *
 * @param <K>
 * @param <V>
 */
public interface ClientDataStreamer<K, V> extends AutoCloseable {
    /** Default operations batch size to sent to cluster for loading. */
    public static final int DFLT_BUFFER_SIZE = 512;

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
     * Note that when this flag is {@code false}, updates will not be propagated to the cache store
     * (i.e. {@link #skipStore()} flag will be set to {@code true} implicitly).
     * <p>
     * This flag is disabled by default (default is {@code false}).
     *
     * @param allowOverwrite Flag value.
     */
    public void allowOverwrite(boolean allowOverwrite);

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
     * Gets size of key-value pairs buffer.
     *
     * @return Buffer size.
     */
    public int bufferSize();

    /**
     * Sets size of key-value pairs buffer. This buffer is used to accumulate entries before sending the batch to
     * the server.
     * <p>
     * This method should be called prior to {@link #addData(Object, Object)} call.
     * <p>
     * If not provided, default value is {@link #DFLT_BUFFER_SIZE}.
     *
     * @param bufSize Buffer size.
     */
    public void bufferSize(int bufSize);

    /**
     * Sets the timeout that is used in the following cases:
     * <ul>
     * <li>Any data addition method can be blocked when data streamer is exhausted.
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
     * streamer will make an attempt to submit all data added so far to cluster.
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
     * streamer will make an attempt to submit all data added so far to cluster.
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
    public IgniteClientFuture<?> future();

    /**
     * Adds key for removal on remote node. Equivalent to {@link #addData(Object, Object) addData(key, null)}.
     *
     * @param key Key.
     * @return Future for this operation.
     *      Note: It may never complete unless {@link #flush()} or {@link #close()} are explicitly called.
     * @throws CacheException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on streamer.
     */
    public IgniteClientFuture<?> removeData(K key) throws CacheException, IgniteInterruptedException, IllegalStateException;

    /**
     * Adds data for streaming on remote node. This method can be called from multiple
     * threads in parallel to speed up streaming if needed.
     * <p>
     * Note that streamer will stream data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the streamer. The data may not be sent until {@link #flush()} or {@link #close()} are called.
     * <p>
     * Note: if {@link IgniteDataStreamer#allowOverwrite()} set to {@code false} (by default)
     * then data streamer will not overwrite existing cache entries for better performance
     * (to change, set {@link IgniteDataStreamer#allowOverwrite(boolean)} to {@code true})
     *
     * @param key Key.
     * @param val Value or {@code null} if respective entry must be removed from cache.
     * @return Future for this operation.
     *      Note: It may never complete unless {@link #flush()} or {@link #close()} are explicitly called.
     * @throws CacheException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on streamer.
     * @throws IgniteDataStreamerTimeoutException If {@code timeout} is exceeded.
     * @see #allowOverwrite()
     */
    public IgniteClientFuture<?> addData(K key, @Nullable V val) throws CacheException, IgniteInterruptedException,
        IllegalStateException, IgniteDataStreamerTimeoutException;

    /**
     * Adds data for streaming on remote node. This method can be called from multiple
     * threads in parallel to speed up streaming if needed.
     * <p>
     * Note that streamer will stream data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the streamer. The data may not be sent until {@link #flush()} or {@link #close()} are called.
     * <p>
     * Note: if {@link IgniteDataStreamer#allowOverwrite()} set to {@code false} (by default)
     * then data streamer will not overwrite existing cache entries for better performance
     * (to change, set {@link IgniteDataStreamer#allowOverwrite(boolean)} to {@code true})
     *
     * @param entry Entry.
     * @return Future for this operation.
     *      Note: It may never complete unless {@link #flush()} or {@link #close()} are explicitly called.
     * @throws CacheException If failed to map key to node.
     * @throws IgniteInterruptedException If thread has been interrupted.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on streamer.
     * @throws IgniteDataStreamerTimeoutException If {@code timeout} is exceeded.
     * @see #allowOverwrite()
     */
    public IgniteClientFuture<?> addData(Map.Entry<K, V> entry) throws CacheException, IgniteInterruptedException,
        IllegalStateException, IgniteDataStreamerTimeoutException;

    /**
     * Adds data for streaming on remote node. This method can be called from multiple
     * threads in parallel to speed up streaming if needed.
     * <p>
     * Note that streamer will stream data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the streamer. The data may not be sent until {@link #flush()} or {@link #close()} are called.
     * <p>
     * Note: if {@link IgniteDataStreamer#allowOverwrite()} set to {@code false} (by default)
     * then data streamer will not overwrite existing cache entries for better performance
     * (to change, set {@link IgniteDataStreamer#allowOverwrite(boolean)} to {@code true})
     *
     * @param entries Collection of entries to be streamed.
     * @return Future for this stream operation.
     *      Note: It may never complete unless {@link #flush()} or {@link #close()} are explicitly called.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on streamer.
     * @throws IgniteDataStreamerTimeoutException If {@code timeout} is exceeded.
     * @see #allowOverwrite()
     */
    public IgniteClientFuture<?> addData(Collection<? extends Map.Entry<K, V>> entries) throws IllegalStateException,
        IgniteDataStreamerTimeoutException;

    /**
     * Adds data for streaming on remote node. This method can be called from multiple
     * threads in parallel to speed up streaming if needed.
     * <p>
     * Note that streamer will stream data concurrently by multiple internal threads, so the
     * data may get to remote nodes in different order from which it was added to
     * the streamer. The data may not be sent until {@link #flush()} or {@link #close()} are called.
     * <p>
     * Note: if {@link IgniteDataStreamer#allowOverwrite()} set to {@code false} (by default)
     * then data streamer will not overwrite existing cache entries for better performance
     * (to change, set {@link IgniteDataStreamer#allowOverwrite(boolean)} to {@code true})
     *
     * @param entries Map to be streamed.
     * @return Future for this stream operation.
     *      Note: It may never complete unless {@link #flush()} or {@link #close()} are explicitly called.
     * @throws IllegalStateException If grid has been concurrently stopped or
     *      {@link #close(boolean)} has already been called on streamer.
     * @throws IgniteDataStreamerTimeoutException If {@code timeout} is exceeded.
     * @see #allowOverwrite()
     */
    public IgniteClientFuture<?> addData(Map<K, V> entries) throws IllegalStateException,
        IgniteDataStreamerTimeoutException;

    /**
     * Streams any remaining data, but doesn't close the streamer. Data can be still added after
     * flush is finished. This method blocks and doesn't allow to add any data until all data
     * is streamed.
     * <p>
     * If another thread is already performing flush, this method will block, wait for
     * another thread to complete flush and exit. If you don't want to wait in this case,
     * use {@link #tryFlush()} method.
     * <p>
     * Note that #flush() guarantees completion of all futures returned by {@link #addData(Object, Object)}, listeners
     * should be tracked separately.
     *
     * @throws CacheException If failed to load data from buffer.
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
     * @throws CacheException If failed to load data from buffer.
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
     * @throws CacheException If failed to close data streamer.
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
