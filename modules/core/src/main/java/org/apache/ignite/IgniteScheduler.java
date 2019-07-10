/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite;

import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.scheduler.SchedulerFuture;

/**
 * Provides functionality for scheduling jobs locally using UNIX cron-based syntax.
 * Instance of {@code GridScheduler} is obtained from grid as follows:
 * <pre name="code" class="java">
 * IgniteScheduler s = Ignition.ignite().scheduler();
 * </pre>
 * <p>
 * Scheduler supports standard UNIX {@code cron} format with optional prefix of
 * <tt>{n1, n2}</tt>, where {@code n1} is delay of scheduling in seconds and
 * {@code n2} is the number of execution. Both parameters are optional.
 * Here's an example of scheduling a closure that broadcasts a message
 * to all nodes five times, once every minute, with initial delay of two seconds:
 * <pre name="code" class="java">
 * SchedulerFuture&lt;?&gt; s = Ignition.ignite().scheduler().scheduleLocal(
 *     new Callable&lt;Object&gt;() {
 *         &#64;Override public Object call() throws IgniteCheckedException {
 *             g.broadcast(new IgniteCallable() {...}).get();
 *         }
 *     },
 *     "{2, 5} * * * * *" // 2 seconds delay with 5 executions only.
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
     * @param r Not {@code null} runnable to execute.
     * @return Future for this execution.
     * @throws NullPointerException if {@code r} is {@code null}.
     * @see #callLocal(Callable)
     * @see org.apache.ignite.lang.IgniteClosure
     */
    public IgniteFuture<?> runLocal(Runnable r);

    /**
     * Executes given closure after the delay.
     * <p>
     * Note that class {@link IgniteRunnable} implements {@link Runnable}
     * @param r Not {@code null} runnable to execute.
     * @param delay Initial delay.
     * @param timeUnit Time granularity.
     * @return java.io.Closeable which can be used to cancel execution.
     * @throws NullPointerException if {@code r} is {@code null}.
     */
    public Closeable runLocal(Runnable r, long delay, TimeUnit timeUnit);

    /**
     * Executes given callable on internal system thread pool asynchronously.
     * <p>
     * Note that class {@link IgniteRunnable} implements {@link Runnable} and class {@link IgniteOutClosure}
     * implements {@link Callable} interface.
     *
     * @param <R> Type of the return value for the closure.
     * @param c Not {@code null} callable to execute.
     * @return Future for this execution.
     * @throws NullPointerException if {@code r} is {@code null}.
     * @see #runLocal(Runnable)
     * @see IgniteOutClosure
     */
    public <R> IgniteFuture<R> callLocal(Callable<R> c);

    /**
     * Schedules job for execution using local <b>cron-based</b> scheduling.
     *
     * @param job Not {@code null} job to schedule to run as a background cron-based job.
     * @param ptrn Scheduling pattern in UNIX cron format with optional prefix <tt>{n1, n2}</tt>
     *      where {@code n1} is delay of scheduling in seconds and {@code n2} is the number of execution. Both
     *      parameters are optional.
     * @return Scheduled execution future.
     * @throws NullPointerException if {@code job} is {@code null}.
     */
    public SchedulerFuture<?> scheduleLocal(Runnable job, String ptrn);

    /**
     * Schedules job for execution using local <b>cron-based</b> scheduling.
     *
     * @param job Not {@code null} job to schedule to run as a background cron-based job.
     * @param ptrn Scheduling pattern in UNIX cron format with optional prefix <tt>{n1, n2}</tt>
     *      where {@code n1} is delay of scheduling in seconds and {@code n2} is the number of execution. Both
     *      parameters are optional.
     * @return Scheduled execution future.
     * @throws NullPointerException if {@code job} is {@code null}.
     */
    public <R> SchedulerFuture<R> scheduleLocal(Callable<R> job, String ptrn);
}
