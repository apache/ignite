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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;

/**
 * Future with simplified exception handling, functional programming support
 * and ability to listen for future completion via functional callback.
 *
 * @param <V> Type of the result for the future.
 */
public interface IgniteFuture<V> {
    /**
     * Synchronously waits for completion of the computation and
     * returns computation result.
     *
     * @return Computation result.
     * @throws IgniteInterruptedException Subclass of {@link IgniteException} thrown if the wait was interrupted.
     * @throws IgniteFutureCancelledException Subclass of {@link IgniteException} thrown if computation was cancelled.
     * @throws IgniteException If computation failed.
     */
    public V get() throws IgniteException;

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
    public V get(long timeout, TimeUnit unit) throws IgniteException;

    /**
     * Cancels this future.
     *
     * @return {@code True} if future was canceled (i.e. was not finished prior to this call).
     * @throws IgniteException If cancellation failed.
     */
    public boolean cancel() throws IgniteException;

    /**
     * Returns {@code true} if this task was cancelled before it completed
     * normally.
     *
     * @return {@code true} if this task was cancelled before it completed
     */
    public boolean isCancelled();

    /**
     * Returns {@code true} if this task completed.
     * <p>
     * Completion may be due to normal termination, an exception, or
     * cancellation - in all of these cases, this method will return
     * {@code true}.
     *
     * @return {@code true} if this task completed.
     */
    public boolean isDone();

    /**
     * Registers a callback to be invoked when the future completes.
     * If the future is already completed, callback will be invoked immediately in the current thread.
     *
     * @param lsnr Listener closure to register. Cannot be {@code null}.
     */
    public void listen(IgniteInClosure<? super IgniteFuture<V>> lsnr);

    /**
     * Registers a callback to be invoked with the specified executor when the future completes.
     *
     * @param lsnr Listener closure to register. Cannot be {@code null}.
     * @param exec Executor to invoke the listener. Cannot be {@code null}.
     */
    public void listenAsync(IgniteInClosure<? super IgniteFuture<V>> lsnr, Executor exec);

    /**
     * Make a chained future to convert result of this future (when complete) into a new format.
     * It is guaranteed that done callback will be called only ONCE.
     *
     * @param doneCb Done callback that is applied to this future when it finishes to produce chained future result.
     * @param <T> Type of the converted result.
     * @return Chained future that finishes after this future completes and done callback is called.
     */
    public <T> IgniteFuture<T> chain(IgniteClosure<? super IgniteFuture<V>, T> doneCb);

    /**
     * Make a chained future to convert result of this future (when complete) into a new format.
     * It is guaranteed that done callback will be called only ONCE.
     *
     * @param doneCb Done callback that is applied to this future when it finishes to produce chained future result.
     * @param exec Executor to run done callback. Cannot be {@code null}.
     * @param <T> Type of the converted result.
     * @return Chained future that finishes after this future completes and done callback is called.
     */
    public <T> IgniteFuture<T> chainAsync(IgniteClosure<? super IgniteFuture<V>, T> doneCb, Executor exec);
}
