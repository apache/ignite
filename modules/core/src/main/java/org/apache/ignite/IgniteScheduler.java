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

import java.util.concurrent.Callable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.scheduler.SchedulerFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Provides functionality for scheduling jobs locally using UNIX cron-based syntax.
 * Instance of {@code GridScheduler} is obtained from grid as follows:
 * <pre name="code" class="java">
 * GridScheduler s = Ignition.ignite().scheduler();
 * </pre>
 * <p>
 * Scheduler supports standard UNIX {@code cron} format with optional prefix of
 * <tt>{n1, n2}</tt>, where {@code n1} is delay of scheduling in seconds and
 * {@code n2} is the number of execution. Both parameters are optional.
 * Here's an example of scheduling a closure that broadcasts a message
 * to all nodes five times, once every minute, with initial delay of two seconds:
 * <pre name="code" class="java">
 * Ignition.ignite().scheduler().scheduleLocal(
 *     GridSchedulerFuture&lt;?&gt; = Ignition.ignite().scheduler().scheduleLocal(new Callable&lt;Object&gt;() {
 *         &#64;Override public Object call() throws IgniteCheckedException {
 *             g.broadcast(new GridCallable() {...}).get();
 *         }
 *     }, "{2, 5} * * * * *" // 2 seconds delay with 5 executions only.
 * );
 * </pre>
 */
public interface IgniteScheduler {
    /**
     * Executes given closure on internal system thread pool asynchronously.
     * <p>
     * Note that class {@link IgniteRunnable} implements {@link Runnable} and class {@link IgniteOutClosure}
     * implements {@link Callable} interface.
     *
     * @param r Runnable to execute. If {@code null} - this method is no-op.
     * @return Future for this execution.
     * @see #callLocal(Callable)
     * @see org.apache.ignite.lang.IgniteClosure
     */
    public IgniteFuture<?> runLocal(@Nullable Runnable r);

    /**
     * Executes given callable on internal system thread pool asynchronously.
     * <p>
     * Note that class {@link IgniteRunnable} implements {@link Runnable} and class {@link IgniteOutClosure}
     * implements {@link Callable} interface.
     *
     * @param c Callable to execute. If {@code null} - this method is no-op.
     * @return Future for this execution.
     * @param <R> Type of the return value for the closure.
     * @see #runLocal(Runnable)
     * @see IgniteOutClosure
     */
    public <R> IgniteFuture<R> callLocal(@Nullable Callable<R> c);

    /**
     * Schedules job for execution using local <b>cron-based</b> scheduling.
     *
     * @param job Job to schedule to run as a background cron-based job.
     *      If {@code null} - this method is no-op.
     * @param ptrn Scheduling pattern in UNIX cron format with optional prefix <tt>{n1, n2}</tt>
     *      where {@code n1} is delay of scheduling in seconds and {@code n2} is the number of execution. Both
     *      parameters are optional.
     * @return Scheduled execution future.
     */
    public SchedulerFuture<?> scheduleLocal(Runnable job, String ptrn);

    /**
     * Schedules job for execution using local <b>cron-based</b> scheduling.
     *
     * @param c Job to schedule to run as a background cron-based job.
     * @param ptrn Scheduling pattern in UNIX cron format with optional prefix <tt>{n1, n2}</tt>
     *      where {@code n1} is delay of scheduling in seconds and {@code n2} is the number of execution. Both
     *      parameters are optional.
     * @return Scheduled execution future.
     */
    public <R> SchedulerFuture<R> scheduleLocal(Callable<R> c, String ptrn);
}