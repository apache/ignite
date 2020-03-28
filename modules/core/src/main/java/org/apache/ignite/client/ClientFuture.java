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

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Thin client future for asynchronous operations.
 */
public interface ClientFuture<R> {
    /**
     * Synchronously waits for completion and returns result.
     *
     * @return Completed future result.
     * @throws ClientException In case of error.
     * @throws CancellationException If the computation was cancelled.
     * @throws InterruptedException If the wait for future completion was interrupted.
     */
    public R get() throws ClientException, CancellationException, InterruptedException;

    /**
     * Synchronously waits for completion and returns result.
     *
     * @param timeout Timeout interval to wait future completes.
     * @param unit Timeout interval unit to wait future completes.
     * @return Completed future result.
     * @throws ClientException In case of error.
     * @throws TimeoutException If timed out before future finishes.
     * @throws CancellationException If the computation was cancelled.
     * @throws InterruptedException If the wait for future completion was interrupted.
     */
    public R get(long timeout, TimeUnit unit)
        throws ClientException, TimeoutException, CancellationException, InterruptedException;

    /**
     * Checks if future is done.
     *
     * @return Whether future is done.
     */
    public boolean isDone();

    /**
     * Cancels this future.
     *
     * @return {@code True} if future was canceled (i.e. was not finished prior to this call).
     * @throws ClientException If cancellation failed.
     */
    public boolean cancel() throws ClientException;

    /**
     * Returns {@code true} if this task was cancelled before it completed
     * normally.
     *
     * @return {@code true} if this task was cancelled before it completed.
     */
    public boolean isCancelled();
}
