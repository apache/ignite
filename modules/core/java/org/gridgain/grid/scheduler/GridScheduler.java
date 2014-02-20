// @java.file.header

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
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridScheduler {
    /**
     * Executes given closure on internal system thread pool asynchronously.
     * <p>
     * Note that class {@link GridRunnable} implements {@link Runnable} and class {@link GridOutClosure}
     * implements {@link Callable} interface.
     *
     * @param r Runnable to execute. If {@code null} - this method is no-op.
     * @return Future for this execution.
     * @see #callLocal(Callable)
     * @see GridClosure
     */
    public GridFuture<?> runLocal(@Nullable Runnable r);

    /**
     * Executes given callable on internal system thread pool asynchronously.
     * <p>
     * Note that class {@link GridRunnable} implements {@link Runnable} and class {@link GridOutClosure}
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
     * Schedules closure for execution using local <b>cron-based</b> scheduling.
     * <p>
     * Here's an example of scheduling a closure that broadcasts a message
     * to all nodes five times, once every minute, with initial delay in two seconds:
     * <pre name="code" class="java">
     * GridGain.grid().scheduleLocal(
     *     new CA() { // CA is a type alias for GridAbsClosure.
     *         &#64;Override public void apply() {
     *             try {
     *                 g.run(BROADCAST, F.println("Hello Node! :)");
     *             }
     *             catch (GridException e) {
     *                 throw new GridClosureException(e);
     *             }
     *         }
     *     }, "{2, 5} * * * * *" // 2 seconds delay with 5 executions only.
     * );
     * </pre>
     * <p>
     * Note that class {@link GridRunnable} implements {@link Runnable} and class {@link GridOutClosure}
     * implements {@link Callable} interface.
     *
     * @param c Closure to schedule to run as a background cron-based job.
     *      If {@code null} - this method is no-op.
     * @param ptrn Scheduling pattern in UNIX cron format with optional prefix <tt>{n1, n2}</tt>
     *      where {@code n1} is delay of scheduling in seconds and {@code n2} is the number of execution. Both
     *      parameters are optional.
     * @return Scheduled execution future.
     * @throws GridException Thrown in case of any errors.
     */
    public GridSchedulerFuture<?> scheduleLocal(@Nullable Runnable c, String ptrn) throws GridException;

    /**
     * Schedules closure for execution using local <b>cron-based</b> scheduling.
     * <p>
     * Here's an example of scheduling a closure that broadcasts a message
     * to all nodes five times, once every minute, with initial delay in two seconds:
     * <pre name="code" class="java">
     * GridGain.grid().scheduleLocal(
     *     new CO<String>() { // CO is a type alias for GridOutClosure.
     *         &#64;Override public String apply() {
     *             try {
     *                 g.run(BROADCAST, F.println("Hello Node! :)");
     *
     *                 return "OK";
     *             }
     *             catch (GridException e) {
     *                 throw new GridClosureException(e);
     *             }
     *         }
     *     }, "{2, 5} * * * * *" // 2 seconds delay with 5 executions only.
     * );
     * </pre>
     * <p>
     * Note that class {@link GridRunnable} implements {@link Runnable} and class {@link GridOutClosure}
     * implements {@link Callable} interface.
     *
     * @param c Closure to schedule to run as a background cron-based job.
     *       If {@code null} - this method is no-op.
     * @param ptrn Scheduling pattern in UNIX cron format with optional prefix <tt>{n1, n2}</tt>
     *      where {@code n1} is delay of scheduling in seconds and {@code n2} is the number of execution. Both
     *      parameters are optional.
     * @return Scheduled execution future.
     * @throws GridException Thrown in case of any errors.
     */
    public <R> GridSchedulerFuture<R> scheduleLocal(@Nullable Callable<R> c, String ptrn) throws GridException;
}
