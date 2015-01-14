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

package org.gridgain.grid.util.nio;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * Reduced variant of {@link org.apache.ignite.lang.IgniteFuture} interface. Removed asynchronous
 * listen methods which require a valid grid kernal context.
 * @param <R> Type of the result for the future.
 */
public interface GridNioFuture<R> {
    /**
     * Synchronously waits for completion of the operation and
     * returns operation result.
     *
     * @return Operation result.
     * @throws GridInterruptedException Subclass of {@link GridException} thrown if the wait was interrupted.
     * @throws IgniteFutureCancelledException Subclass of {@link GridException} throws if operation was cancelled.
     * @throws IgniteCheckedException If operation failed.
     * @throws IOException If IOException occurred while performing operation.
     */
    public R get() throws IOException, IgniteCheckedException;

    /**
     * Synchronously waits for completion of the operation for
     * up to the timeout specified and returns operation result.
     * This method is equivalent to calling {@link #get(long, TimeUnit) get(long, TimeUnit.MILLISECONDS)}.
     *
     * @param timeout The maximum time to wait in milliseconds.
     * @return Operation result.
     * @throws GridInterruptedException Subclass of {@link GridException} thrown if the wait was interrupted.
     * @throws IgniteFutureTimeoutException Subclass of {@link GridException} thrown if the wait was timed out.
     * @throws IgniteFutureCancelledException Subclass of {@link GridException} throws if operation was cancelled.
     * @throws IgniteCheckedException If operation failed.
     * @throws IOException If IOException occurred while performing operation.
     */
    public R get(long timeout) throws IOException, IgniteCheckedException;

    /**
     * Synchronously waits for completion of the operation for
     * up to the timeout specified and returns operation result.
     *
     * @param timeout The maximum time to wait.
     * @param unit The time unit of the {@code timeout} argument.
     * @return Operation result.
     * @throws GridInterruptedException Subclass of {@link GridException} thrown if the wait was interrupted.
     * @throws IgniteFutureTimeoutException Subclass of {@link GridException} thrown if the wait was timed out.
     * @throws IgniteFutureCancelledException Subclass of {@link GridException} throws if operation was cancelled.
     * @throws IgniteCheckedException If operation failed.
     * @throws IOException If IOException occurred while performing operation.
     */
    public R get(long timeout, TimeUnit unit) throws IOException, IgniteCheckedException;

    /**
     * Cancels this future.
     *
     * @return {@code True} if future was canceled (i.e. was not finished prior to this call).
     * @throws IgniteCheckedException If cancellation failed.
     */
    public boolean cancel() throws IgniteCheckedException;

    /**
     * Checks if operation is done.
     *
     * @return {@code True} if operation is done, {@code false} otherwise.
     */
    public boolean isDone();

    /**
     * Returns {@code true} if this operation was cancelled before it completed normally.
     *
     * @return {@code True} if this operation was cancelled before it completed normally.
     */
    public boolean isCancelled();

    /**
     * Registers listener closure to be asynchronously notified whenever future completes.
     *
     * @param lsnr Listener closure to register. If not provided - this method is no-op.
     */
    public void listenAsync(@Nullable IgniteInClosure<? super GridNioFuture<R>> lsnr);

    /**
     * Sets flag indicating that message send future was created in thread that was processing a message.
     *
     * @param msgThread {@code True} if future was created in thread that is processing message.
     */
    public void messageThread(boolean msgThread);

    /**
     * @return {@code True} if future was created in thread that was processing message.
     */
    public boolean messageThread();

    /**
     * @return {@code True} if skip recovery for this operation.
     */
    public boolean skipRecovery();
}
