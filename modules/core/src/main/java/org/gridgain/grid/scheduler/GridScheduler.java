/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.scheduler;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

/**
 * Provides functionality for scheduling jobs locally using UNIX cron-based syntax.
 * Instance of {@code GridScheduler} is obtained from grid as follows:
 * <pre name="code" class="java">
 * GridScheduler s = GridGain.grid().scheduler();
 * </pre>
 * <p>
 * Scheduler supports standard UNIX {@code cron} format with optional prefix of
 * <tt>{n1, n2}</tt>, where {@code n1} is delay of scheduling in seconds and
 * {@code n2} is the number of execution. Both parameters are optional.
 * Here's an example of scheduling a closure that broadcasts a message
 * to all nodes five times, once every minute, with initial delay of two seconds:
 * <pre name="code" class="java">
 * GridGain.grid().scheduler().scheduleLocal(
 *     GridSchedulerFuture&lt;?&gt; = GridGain.grid().scheduler().scheduleLocal(new Callable&lt;Object&gt;() {
 *         &#64;Override public Object call() throws GridException {
 *             g.broadcast(new GridCallable() {...}).get();
 *         }
 *     }, "{2, 5} * * * * *" // 2 seconds delay with 5 executions only.
 * );
 * </pre>
 */
public interface GridScheduler {
    /**
     * Executes given closure on internal system thread pool asynchronously.
     * <p>
     * Note that class {@link org.apache.ignite.lang.IgniteRunnable} implements {@link Runnable} and class {@link GridOutClosure}
     * implements {@link Callable} interface.
     *
     * @param r Runnable to execute. If {@code null} - this method is no-op.
     * @return Future for this execution.
     * @see #callLocal(Callable)
     * @see org.gridgain.grid.lang.IgniteClosure
     */
    public GridFuture<?> runLocal(@Nullable Runnable r);

    /**
     * Executes given callable on internal system thread pool asynchronously.
     * <p>
     * Note that class {@link org.apache.ignite.lang.IgniteRunnable} implements {@link Runnable} and class {@link GridOutClosure}
     * implements {@link Callable} interface.
     *
     * @param c Callable to execute. If {@code null} - this method is no-op.
     * @return Future for this execution.
     * @param <R> Type of the return value for the closure.
     * @see #runLocal(Runnable)
     * @see GridOutClosure
     */
    public <R> GridFuture<R> callLocal(@Nullable Callable<R> c);

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
    public GridSchedulerFuture<?> scheduleLocal(Runnable job, String ptrn);

    /**
     * Schedules job for execution using local <b>cron-based</b> scheduling.
     *
     * @param c Job to schedule to run as a background cron-based job.
     * @param ptrn Scheduling pattern in UNIX cron format with optional prefix <tt>{n1, n2}</tt>
     *      where {@code n1} is delay of scheduling in seconds and {@code n2} is the number of execution. Both
     *      parameters are optional.
     * @return Scheduled execution future.
     */
    public <R> GridSchedulerFuture<R> scheduleLocal(Callable<R> c, String ptrn);
}
