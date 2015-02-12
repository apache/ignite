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

package org.apache.ignite.lang;

import org.apache.ignite.*;

import java.util.concurrent.*;

/**
 * Extension for standard {@link Future} interface. It adds simplified exception handling,
 * functional programming support and ability to listen for future completion via functional
 * callback.
 * @param <V> Type of the result for the future.
 */
public interface IgniteFuture<V> extends Future<V> {
    /**
     * Synchronously waits for completion of the computation and
     * returns computation result.
     *
     * @return Computation result.
     * @throws IgniteInterruptedException Subclass of {@link IgniteException} thrown if the wait was interrupted.
     * @throws IgniteFutureCancelledException Subclass of {@link IgniteException} thrown if computation was cancelled.
     * @throws IgniteException If computation failed.
     */
    @Override public V get() throws IgniteException;

    /**
     * Synchronously waits for completion of the computation for
     * up to the timeout specified and returns computation result.
     * This method is equivalent to calling {@link #get(long, TimeUnit) get(long, TimeUnit.MILLISECONDS)}.
     *
     * @param timeout The maximum time to wait in milliseconds.
     * @return Computation result.
     * @throws IgniteInterruptedException Subclass of {@link IgniteException} thrown if the wait was interrupted.
     * @throws IgniteFutureCancelledException Subclass of {@link IgniteException} thrown if computation was cancelled.
     * @throws IgniteFutureTimeoutException Subclass of {@link IgniteException} thrown if the wait was timed out.
     * @throws IgniteException If computation failed.
     */
    public V get(long timeout) throws IgniteException;

    /**
     * Synchronously waits for completion of the computation for
     * up to the timeout specified and returns computation result.
     *
     * @param timeout The maximum time to wait.
     * @param unit The time unit of the {@code timeout} argument.
     * @return Computation result.
     * @throws IgniteInterruptedException Subclass of {@link IgniteException} thrown if the wait was interrupted.
     * @throws IgniteFutureCancelledException Subclass of {@link IgniteException} thrown if computation was cancelled.
     * @throws IgniteFutureTimeoutException Subclass of {@link IgniteException} thrown if the wait was timed out.
     * @throws IgniteException If computation failed.
     */
    @Override public V get(long timeout, TimeUnit unit) throws IgniteException;

    /**
     * Cancels this future.
     *
     * @return {@code True} if future was canceled (i.e. was not finished prior to this call).
     * @throws IgniteException If cancellation failed.
     */
    public boolean cancel() throws IgniteException;

    /**
     * Gets start time for this future.
     *
     * @return Start time for this future.
     */
    public long startTime();

    /**
     * Gets duration in milliseconds between start of the future and current time if future
     * is not finished, or between start and finish of this future.
     *
     * @return Time in milliseconds this future has taken to execute.
     */
    public long duration();

    /**
     * Flag to turn on or off synchronous listener notification. If this flag is {@code true}, then
     * upon future completion the notification may happen in the same thread that created
     * the future. This becomes especially important when adding listener to a future that
     * is already {@code done} - if this flag is {@code true}, then listener will be
     * immediately notified within the same thread.
     * <p>
     * Default value is {@code false}. To change the default, set
     * {@link IgniteSystemProperties#IGNITE_FUT_SYNC_NOTIFICATION} system property to {@code true}.
     *
     * @param syncNotify Flag to turn on or off synchronous listener notification.
     */
    public void syncNotify(boolean syncNotify);

    /**
     * Gets value of synchronous listener notification flag. If this flag is {@code true}, then
     * upon future completion the notification may happen in the same thread that created
     * the future. This becomes especially important when adding listener to a future that
     * is already {@code done} - if this flag is {@code true}, then listener will be
     * immediately notified within the same thread.
     * <p>
     * Default value is {@code false}. To change the default, set
     * {@link IgniteSystemProperties#IGNITE_FUT_SYNC_NOTIFICATION} system property to {@code true}.
     *
     * @return Synchronous listener notification flag.
     */
    public boolean syncNotify();

    /**
     * Flag to turn on or off concurrent listener notification. This flag comes into play only
     * when a future has more than one listener subscribed to it. If this flag is {@code true},
     * then all listeners will be notified concurrently by different threads; otherwise,
     * listeners will be notified one after another within one thread (depending on
     * {@link #syncNotify()} flag, these notifications may happen either in the same thread which
     * started the future, or in a different thread).
     * <p>
     * Default value is {@code false}. To change the default, set
     * {@link IgniteSystemProperties#IGNITE_FUT_CONCURRENT_NOTIFICATION} system property to {@code true}.
     *
     * @param concurNotify Flag to turn on or off concurrent listener notification.
     */
    public void concurrentNotify(boolean concurNotify);

    /**
     * Gets value concurrent listener notification flag. This flag comes into play only
     * when a future has more than one listener subscribed to it. If this flag is {@code true},
     * then all listeners will be notified concurrently by different threads; otherwise,
     * listeners will be notified one after another within one thread (depending on
     * {@link #syncNotify()} flag, these notifications may happen either in the same thread which
     * started the future, or in a different thread).
     * <p>
     * Default value is {@code false}. To change the default, set
     * {@link IgniteSystemProperties#IGNITE_FUT_CONCURRENT_NOTIFICATION} system property to {@code true}.
     *
     * @return Concurrent listener notification flag
     */
    public boolean concurrentNotify();

    /**
     * Registers listener closure to be asynchronously notified whenever future completes.
     *
     * @param lsnr Listener closure to register. If not provided - this method is no-op.
     */
    public void listenAsync(IgniteInClosure<? super IgniteFuture<V>> lsnr);

    /**
     * Removes given listener from the future.
     *
     * @param lsnr Listener to remove.
     */
    public void stopListenAsync(IgniteInClosure<? super IgniteFuture<V>> lsnr);

    /**
     * Make a chained future to convert result of this future (when complete) into a new format.
     * It is guaranteed that done callback will be called only ONCE.
     *
     * @param doneCb Done callback that is applied to this future when it finishes to produce chained future result.
     * @return Chained future that finishes after this future completes and done callback is called.
     */
    public <T> IgniteFuture<T> chain(IgniteClosure<? super IgniteFuture<V>, T> doneCb);
}
