// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.executor;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * An {@link ExecutorService} that executes each submitted task in grid
 * through {@link Grid} instance, normally configured using
 * {@link GridProjection#executor(org.gridgain.grid.lang.GridPredicate[])} ()} method.
 * {@code GridExecutorService} delegates commands execution to already
 * started {@link Grid} instance. Every submitted task will be serialized and
 * transfered to any node in grid.
 * <p>
 * All submitted tasks must implement {@link Serializable} interface.
 * <p>
 * Note, that GridExecutorService implements ExecutorService from JDK 1.5.
 * If you have problems with compilation for JDK 1.6 and above you need to apply
 * some changes (see <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6267833">http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6267833</a>)
 * <p>
 * Change signature for methods {@link GridExecutorService#invokeAll(Collection)},
 * {@link GridExecutorService#invokeAll(Collection, long, TimeUnit)},
 * {@link GridExecutorService#invokeAny(Collection)},
 * {@link GridExecutorService#invokeAny(Collection, long, TimeUnit)} to
 * <pre name="code" class="java">
 * public class GridExecutorService implements ExecutorService {
 * ...
 *     public &lt;T&gt; List&lt;Future&lt;T&gt;&gt; invokeAll(Collection&lt;? extends Callable&lt;T&gt;&gt; tasks) throws InterruptedException {
 *         ...
 *     }
 *
 *     public &lt;T&gt; List&lt;Future&lt;T&gt;&gt; invokeAll(Collection&lt;? extends Callable&lt;T&gt;&gt; tasks, long timeout, TimeUnit unit)
 *         throws InterruptedException {
 *         ...
 *     }
 *
 *     public &lt;T&gt; T invokeAny(Collection&lt;? extends Callable&lt;T&gt;&gt; tasks) throws InterruptedException, ExecutionException {
 *         ...
 *     }
 *
 *     public &lt;T&gt; T invokeAny(Collection&lt;? extends Callable&lt;T&gt;&gt; tasks, long timeout, TimeUnit unit)
 *         throws InterruptedException, ExecutionException, TimeoutException {
 *     }
 *     ...
 * }
 * </pre>
 *
 * @author @java.author
 * @version @java.version
 */
public class GridExecutorService extends GridMetadataAwareAdapter implements ExecutorService, Externalizable {
    /** */
    private static final ThreadLocal<GridProjection> stash = new ThreadLocal<>();

    /** Projection. */
    private GridProjection prj;

    /** Logger. */
    private GridLogger log;

    /** Whether service is being stopped or not. */
    private boolean isBeingShutdown;

    /** List of executing or scheduled for execution tasks. */
    private List<GridComputeTaskFuture<?>> futs = new ArrayList<>();

    /** Rejected or completed tasks listener. */
    private TaskTerminateListener lsnr = new TaskTerminateListener<>();

    /** */
    private final Object mux = new Object();

    /**
     * No-arg constructor is required by externalization.
     */
    public GridExecutorService() {
        // No-op.
    }

    /**
     * Creates executor service.
     *
     * @param prj Projection.
     * @param log Grid logger.
     */
    public GridExecutorService(GridProjection prj, GridLogger log) {
        assert prj != null;
        assert log != null;

        this.prj = prj;
        this.log = log.getLogger(GridExecutorService.class);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(prj);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        stash.set((GridProjection) in.readObject());
    }

    /**
     * Reconstructs object on demarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of demarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            return stash.get().compute().executor();
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        synchronized (mux) {
            if (isBeingShutdown)
                return;

            isBeingShutdown = true;
        }
    }

    /** {@inheritDoc} */
    @Override public List<Runnable> shutdownNow() {
        List<GridComputeTaskFuture<?>> cpFuts;

        // Cancel all tasks.
        synchronized (mux) {
            cpFuts = new ArrayList<>(futs);

            isBeingShutdown = true;
        }

        for (GridComputeTaskFuture<?> task : cpFuts) {
            try {
                task.cancel();
            }
            catch (GridException e) {
                U.error(log, "Failed to cancel task: " + task, e);
            }
        }

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public boolean isShutdown() {
        synchronized (mux) {
            return isBeingShutdown;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminated() {
        synchronized (mux) {
            return isBeingShutdown && futs.isEmpty();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long now = U.currentTimeMillis();

        timeout = TimeUnit.MILLISECONDS.convert(timeout, unit);

        long end = timeout == 0 ? Long.MAX_VALUE : timeout + now;

        // Prevent overflow.
        if (end < 0)
            end = Long.MAX_VALUE;

        List<GridComputeTaskFuture<?>> locTasks;

        // Cancel all tasks.
        synchronized (mux) {
            locTasks = new ArrayList<>(futs);
        }

        Iterator<GridComputeTaskFuture<?>> iter = locTasks.iterator();

        while (iter.hasNext() && now < end) {
            GridComputeTaskFuture<?> fut = iter.next();

            try {
                fut.get(end - now);
            }
            catch (GridComputeTaskTimeoutException e) {
                U.error(log, "Failed to get task result: " + fut, e);

                return false;
            }
            catch (GridException e) {
                U.error(log, "Failed to get task result: " + fut, e);

                if (e.getCause() instanceof InterruptedException)
                    throw new InterruptedException("Got interrupted while waiting for task completion.");
            }

            now = U.currentTimeMillis();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public <T> Future<T> submit(Callable<T> task) {
        A.notNull(task, "task != null");

        checkShutdown();

        deployTask(GridExecutorCallableTask.class, task);

        return addFuture(prj.compute().execute(new GridExecutorCallableTask<T>(task.getClass()), task));
    }

    /** {@inheritDoc} */
    @Override public <T> Future<T> submit(Runnable task, T res) {
        A.notNull(task, "task != null");

        checkShutdown();

        deployTask(GridExecutorCallableTask.class, task);

        return addFuture(prj.compute().execute(new GridExecutorCallableTask<T>(task.getClass()),
            new GridExecutorRunnableAdapter<>(task, res)));
    }

    /** {@inheritDoc} */
    @Override public Future<?> submit(Runnable task) {
        A.notNull(task, "task != null");

        checkShutdown();

        deployTask(GridExecutorRunnableTask.class, task);

        return addFuture(prj.compute().execute(new GridExecutorRunnableTask(task.getClass()), task));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note, for compilation with JDK 1.6 necessary to change method signature
     * (note the {@code &lt;? extends T&gt;} clause).
     * <pre name="code" class="java">
     *     ...
     *     public &lt;T&gt; List&lt;Future&lt;T&gt;&gt; invokeAll(Collection&lt;? extends Callable&lt;T&gt;&gt; tasks) throws InterruptedException {
     *         // Method body.
     *     }
     *     ...
     * </pre>
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return invokeAll(tasks, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note, for compilation with JDK 1.6 necessary to change method signature
     * (note the {@code &lt;? extends T&gt;} clause).
     * <pre name="code" class="java">
     *     public &lt;T&gt; List&lt;Future&lt;T&gt;&gt; invokeAll(Collection&lt;? extends Callable&lt;T&gt;&gt; tasks, long timeout, TimeUnit unit)
     *         throws InterruptedException {
     *         ...
     *     }
     * </pre>
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException {
        A.notNull(tasks, "tasks != null");
        A.ensure(timeout >= 0, "timeout >= 0");
        A.notNull(unit, "unit != null");

        long now = U.currentTimeMillis();

        timeout = TimeUnit.MILLISECONDS.convert(timeout, unit);

        long end = timeout == 0 ? Long.MAX_VALUE : timeout + now;

        // Prevent overflow.
        if (end < 0)
            end = Long.MAX_VALUE;

        checkShutdown();

        Collection<GridComputeTaskFuture<T>> taskFuts = new ArrayList<>();

        for (Callable<T> task : tasks) {
            deployTask(GridExecutorCallableTask.class, task);

            // Execute task without predefined timeout.
            // GridFuture.cancel() will be called if timeout elapsed.
            GridComputeTaskFuture<T> fut = prj.compute().execute(new GridExecutorCallableTask<T>(task.getClass()), task);

            taskFuts.add(fut);

            now = U.currentTimeMillis();
        }

        boolean isInterrupted = false;

        for (GridComputeTaskFuture<T> fut : taskFuts) {
            if (!isInterrupted && now < end) {
                try {
                    fut.get(end - now);
                }
                catch (GridComputeTaskTimeoutException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Timeout occurred during getting task result: " + fut);

                    cancelFuture(fut);
                }
                catch (GridException e) {
                    if (e.getCause() instanceof InterruptedException) {
                        // This invokeAll() method was interrupted (therefore, need to cancel all tasks).
                        // Note: that execution may be interrupted on remote node. Possible bug.
                        isInterrupted = true;

                        cancelFuture(fut);
                    }
                }
            }

            now = U.currentTimeMillis();
        }

        // Throw exception if any task wait was interrupted.
        if (isInterrupted)
            throw new InterruptedException("Got interrupted while waiting for tasks invocation.");

        List<Future<T>> futs = new ArrayList<>(taskFuts.size());

        // Convert futures.
        for (GridComputeTaskFuture<T> fut : taskFuts) {
            // Per executor service contract any task that was not completed
            // should be cancelled upon return.
            if (!fut.isDone())
                cancelFuture(fut);

            futs.add(new TaskFutureWrapper<>(fut));
        }

        return futs;
    }

    /**
     * Cancels given future.
     *
     * @param fut Future to cancel.
     */
    private void cancelFuture(GridFuture<?> fut) {
        try {
            fut.cancel();
        }
        catch (GridException e) {
            U.error(log, "Failed to cancel task: " + fut, e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note, for compilation with JDK 1.6 necessary to change method signature
     * (note the {@code &lt;? extends T&gt;} clause).
     * <pre name="code" class="java">
     *     ...
     *     public &lt;T&gt; T invokeAny(Collection&lt;? extends Callable&lt;T&gt;&gt; tasks) throws InterruptedException, ExecutionException {
     *         // Method body.
     *     }
     *     ...
     * </pre>
     */
    @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
        ExecutionException {
        try {
            return invokeAny(tasks, 0, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
            throw new ExecutionException("Timeout occurred during commands execution.", e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note, for compilation with JDK 1.6 necessary to change method signature
     * (note the {@code &lt;? extends T&gt;} clause).
     * <pre name="code" class="java">
     *     ...
     *     public &lt;T&gt; T invokeAny(Collection&lt;? extends Callable&lt;T&gt;&gt; tasks, long timeout, TimeUnit unit)
     *         throws InterruptedException, ExecutionException, TimeoutException {
     *     }
     *     ...
     * </pre>
     */
    @SuppressWarnings({"MethodWithTooExceptionsDeclared"})
    @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        A.notNull(tasks, "tasks != null");
        A.ensure(!tasks.isEmpty(), "!tasks.isEmpty()");
        A.ensure(timeout >= 0, "timeout >= 0");
        A.notNull(unit, "unit != null");

        long now = System.currentTimeMillis();

        timeout = TimeUnit.MILLISECONDS.convert(timeout, unit);

        long end = timeout == 0 ? Long.MAX_VALUE : timeout + now;

        // Prevent overflow.
        if (end < 0)
            end = Long.MAX_VALUE;

        checkShutdown();

        Collection<GridComputeTaskFuture<T>> taskFuts = new ArrayList<>();

        for (Callable<T> cmd : tasks) {
            // Execute task with predefined timeout.
            GridComputeTaskFuture<T> fut = prj.compute().execute(new GridExecutorCallableTask<T>(cmd.getClass()), cmd);

            taskFuts.add(fut);
        }

        T res = null;

        boolean isInterrupted = false;
        boolean isResRcvd = false;

        int errCnt = 0;

        for (GridComputeTaskFuture<T> fut : taskFuts) {
            now = U.currentTimeMillis();

            boolean cancel = false;

            if (!isInterrupted && !isResRcvd && now < end) {
                try {
                    res = fut.get(end - now);

                    isResRcvd = true;

                    // Cancel next tasks (avoid current task cancellation below in loop).
                    continue;
                }
                catch (GridFutureTimeoutException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Timeout occurred during getting task result: " + fut);

                    cancel = true;
                }
                catch (GridException e) {
                    // This invokeAll() method was interrupted (therefore, need to cancel all tasks).
                    // Note: that execution may be interrupted on remote node. Possible bug.
                    if (e.getCause() instanceof InterruptedException)
                        isInterrupted = true;
                    else
                        errCnt++;
                }
            }

            // Cancel active task if any task interrupted, timeout elapsed or received task result before.
            if ((isInterrupted || isResRcvd || cancel) && !fut.isDone())
                cancelFuture(fut);
        }

        // Throw exception if any task wait was interrupted.
        if (isInterrupted)
            throw new InterruptedException("Got interrupted while waiting for tasks invocation.");

        // If every task failed - throw execution exception
        // per executor service contract.
        if (!isResRcvd && taskFuts.size() == errCnt)
            throw new ExecutionException("Failed to get any task completion.", null);

        // In all other cases with no results received by the time timeout elapsed -
        // throw timeout exception per executor service contract.
        if (!isResRcvd)
            throw new TimeoutException("Timeout occurred during tasks invocation.");

        return res;
    }

    /** {@inheritDoc} */
    @Override public void execute(Runnable cmd) {
        A.notNull(cmd, "cmd != null");

        checkShutdown();

        deployTask(GridExecutorRunnableTask.class, cmd);

        addFuture(prj.compute().execute(new GridExecutorRunnableTask(cmd.getClass()), cmd));
    }

    /**
     * Checks if service is being shutdown.
     */
    private void checkShutdown() {
        synchronized (mux) {
            if (isBeingShutdown)
                throw new RejectedExecutionException("Failed to execute command during executor shutdown.");
        }
    }

    /**
     * @param <T> Type of command result.
     * @param fut Future to add.
     * @return Future for command.
     */
    @SuppressWarnings("unchecked")
    private <T> Future<T> addFuture(GridComputeTaskFuture<T> fut) {
        synchronized (mux) {
            if (!fut.isDone()) {
                fut.listenAsync(lsnr);

                futs.add(fut);
            }

            return new TaskFutureWrapper<>(fut);
        }
    }

    /**
     * @param taskCls Task class.
     * @param cmd Command to deploy.
     */
    @SuppressWarnings("unchecked")
    private void deployTask(Class<? extends GridComputeTask> taskCls, Object cmd) {
        try {
            prj.compute().localDeployTask(taskCls, cmd.getClass().getClassLoader());
        }
        catch (GridException e) {
            throw new RejectedExecutionException("Failed to deploy command: " + cmd, e);
        }
    }

    /**
     * Listener to track tasks.
     *
     * @author @java.author
     */
    private class TaskTerminateListener<T> extends GridInClosure<GridComputeTaskFuture<T>> {
        /** {@inheritDoc} */
        @Override public void apply(GridComputeTaskFuture<T> taskFut) {
            synchronized (mux) {
                futs.remove(taskFut);
            }
        }
    }

    /**
     * Wrapper for {@link org.gridgain.grid.compute.GridComputeTaskFuture}.
     * Used for compatibility {@link Future} interface.
     *
     * @author @java.author
     * @param <T> The result type of the {@link Future} argument.
     */
    private class TaskFutureWrapper<T> implements Future<T> {
        /** */
        private final GridComputeTaskFuture<T> fut;

        /**
         * Creates wrapper.
         *
         * @param fut Grid future.
         */
        TaskFutureWrapper(GridComputeTaskFuture<T> fut) {
            assert fut != null;

            this.fut = fut;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel(boolean mayInterruptIfRunning) {
            try {
                fut.cancel();
            }
            catch (GridException e) {
                U.error(log, "Failed to cancel task: " + fut, e);
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            return fut.isCancelled();
        }

        /** {@inheritDoc} */
        @Override public boolean isDone() {
            return fut.isDone();
        }

        /** {@inheritDoc} */
        @Override public T get() throws ExecutionException {
            try {
                T res = fut.get();

                if (fut.isCancelled())
                    throw new CancellationException("Task was cancelled: " + fut);

                return res;
            }
            catch (GridException e) {
                // Task cancellation may cause throwing exception.
                if (fut.isCancelled()) {
                    RuntimeException ex = new CancellationException("Task was cancelled: " + fut);

                    ex.initCause(e);

                    throw ex;
                }

                throw new ExecutionException("Failed to get task result: " + fut, e);
            }
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"MethodWithTooExceptionsDeclared"})
        @Override public T get(long timeout, TimeUnit unit) throws ExecutionException, TimeoutException {
            A.ensure(timeout >= 0, "timeout >= 0");
            A.notNull(unit, "unit != null");

            try {
                T res = fut.get(unit.toMillis(timeout));

                if (fut.isCancelled())
                    throw new CancellationException("Task was cancelled: " + fut);

                return res;
            }
            catch (GridFutureTimeoutException e) {
                TimeoutException e2 = new TimeoutException();

                e2.initCause(e);

                throw e2;
            }
            catch (GridComputeTaskTimeoutException e) {
                throw new ExecutionException("Task execution timed out during waiting for task result: " + fut, e);
            }
            catch (GridException e) {
                // Task cancellation may cause throwing exception.
                if (fut.isCancelled()) {
                    RuntimeException ex = new CancellationException("Task was cancelled: " + fut);

                    ex.initCause(e);

                    throw ex;
                }

                throw new ExecutionException("Failed to get task result.", e);
            }
        }
    }
}
