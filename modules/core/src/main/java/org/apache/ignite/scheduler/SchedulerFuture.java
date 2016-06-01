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

package org.apache.ignite.scheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureTimeoutException;

/**
 * Future for cron-based scheduled execution. This future is returned
 * when calling {@link IgniteScheduler#scheduleLocal(Callable, String)} or
 * {@link IgniteScheduler#scheduleLocal(Runnable, String)} methods.
 */
public interface SchedulerFuture<R> extends IgniteFuture<R> {
    /**
     * Gets scheduled task ID.
     *
     * @return ID of scheduled task.
     */
    public String id();

    /**
     * Gets scheduling pattern.
     *
     * @return Scheduling pattern.
     */
    public String pattern();

    /**
     * Gets time in milliseconds at which this future was created.
     *
     * @return Time in milliseconds at which this future was created.
     */
    public long createTime();

    /**
     * Gets start time of last execution ({@code 0} if never started).
     *
     * @return Start time of last execution.
     */
    public long lastStartTime();

    /**
     * Gets finish time of last execution ({@code 0} if first execution has not finished).
     *
     * @return Finish time of last execution ({@code 0} if first execution has not finished).
     */
    public long lastFinishTime();

    /**
     * Gets average execution time in milliseconds since future was created.
     *
     * @return Average execution time in milliseconds since future was created.
     */
    public double averageExecutionTime();

    /**
     * Gets last interval between scheduled executions. If first execution has
     * not yet happened, then returns time passed since creation of this future.
     *
     * @return Last interval between scheduled executions.
     */
    public long lastIdleTime();

    /**
     * Gets average idle time for this scheduled task.
     *
     * @return Average idle time for this scheduled task.
     */
    public double averageIdleTime();

    /**
     * Gets an array of the next execution times after passed {@code start} timestamp.
     *
     * @param cnt Array length.
     * @param start Start timestamp.
     * @return Array of the next execution times in milliseconds.
     * @throws IgniteException Thrown in case of any errors.
     */
    public long[] nextExecutionTimes(int cnt, long start) throws IgniteException;

    /**
     * Gets total count of executions this task has already completed.
     *
     * @return Total count of executions this task has already completed.
     */
    public int count();

    /**
     * Returns {@code true} if scheduled task is currently running.
     *
     * @return {@code True} if scheduled task is currently running.
     */
    public boolean isRunning();

    /**
     * Gets next execution time of scheduled task.
     *
     * @return Next execution time in milliseconds.
     * @throws IgniteException Thrown in case of any errors.
     */
    public long nextExecutionTime() throws IgniteException;

    /**
     * Gets result of the last execution of scheduled task, or
     * {@code null} if task has not been executed, or has not
     * produced a result yet.
     *
     * @return Result of the last execution, or {@code null} if
     *      there isn't one yet.
     * @throws IgniteException If last execution resulted in exception.
     */
    public R last() throws IgniteException;

    /**
     * Waits for the completion of the next scheduled execution and returns its result.
     *
     * @return Result of the next execution.
     * @throws CancellationException {@inheritDoc}
     * @throws IgniteInterruptedException {@inheritDoc}
     * @throws IgniteException {@inheritDoc}
     */
    @Override public R get();

    /**
     * Waits for the completion of the next scheduled execution for
     * specified amount of time and returns its result.
     *
     * @param timeout {@inheritDoc}
     * @param unit {@inheritDoc}
     * @return The computed result of the next execution.
     * @throws CancellationException {@inheritDoc}
     * @throws IgniteInterruptedException {@inheritDoc}
     * @throws IgniteFutureTimeoutException {@inheritDoc}
     * @throws IgniteException {@inheritDoc}
     */
    @Override public R get(long timeout, TimeUnit unit);
}