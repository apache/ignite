/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.executor;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * An {@link ExecutorService} that executes each submitted task in grid
 * through {@link org.apache.ignite.Ignite} instance, normally configured using
 * {@link org.apache.ignite.Ignite#executorService()} method.
 * {@code GridExecutorService} delegates commands execution to already
 * started {@link org.apache.ignite.Ignite} instance. Every submitted task will be serialized and
 * transferred to any node in grid.
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
 */
public class GridExecutorService extends GridMetadataAwareAdapter implements ExecutorService, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Projection. */
    private ClusterGroupAdapter prj;

    /** Compute. */
    private GridCompute comp;

    /** Logger. */
    private GridLogger log;

    /** Whether service is being stopped or not. */
    private boolean isBeingShutdown;

    /** List of executing or scheduled for execution tasks. */
    private List<IgniteFuture<?>> futs = new ArrayList<>();

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
    public GridExecutorService(ClusterGroupAdapter prj, GridLogger log) {
        assert prj != null;
        assert log != null;

        this.prj = prj;
        this.log = log.getLogger(GridExecutorService.class);

        comp = prj.compute().enableAsync();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(prj);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        prj = (ClusterGroupAdapter)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        return prj.executorService();
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
        List<IgniteFuture<?>> cpFuts;

        // Cancel all tasks.
        synchronized (mux) {
            cpFuts = new ArrayList<>(futs);

            isBeingShutdown = true;
        }

        for (IgniteFuture<?> task : cpFuts) {
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

        List<IgniteFuture<?>> locTasks;

        // Cancel all tasks.
        synchronized (mux) {
            locTasks = new ArrayList<>(futs);
        }

        Iterator<IgniteFuture<?>> iter = locTasks.iterator();

        while (iter.hasNext() && now < end) {
            IgniteFuture<?> fut = iter.next();

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

        assert comp.isAsync();

        try {
            comp.call(task);

            return addFuture(comp.<T>future());
        }
        catch (GridException e) {
            // Should not be thrown since uses asynchronous execution.
            return addFuture(new GridFinishedFutureEx<T>(e));
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Future<T> submit(Runnable task, final T res) {
        A.notNull(task, "task != null");

        checkShutdown();

        assert comp.isAsync();

        try {
            comp.run(task);

            IgniteFuture<T> fut = comp.future().chain(new CX1<IgniteFuture<?>, T>() {
                @Override public T applyx(IgniteFuture<?> fut) throws GridException {
                    fut.get();

                    return res;
                }
            });

            return addFuture(fut);
        }
        catch (GridException e) {
            // Should not be thrown since uses asynchronous execution.
            return addFuture(new GridFinishedFutureEx<T>(e));
        }
    }

    /** {@inheritDoc} */
    @Override public Future<?> submit(Runnable task) {
        A.notNull(task, "task != null");

        checkShutdown();

        assert comp.isAsync();

        try {
            comp.run(task);

            return addFuture(comp.future());
        }
        catch (GridException e) {
            // Should not be thrown since uses asynchronous execution.
            return addFuture(new GridFinishedFutureEx<>(e));
        }
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

        Collection<IgniteFuture<T>> taskFuts = new ArrayList<>();

        assert comp.isAsync();

        for (Callable<T> task : tasks) {
            // Execute task without predefined timeout.
            // GridFuture.cancel() will be called if timeout elapsed.
            IgniteFuture<T> fut;

            try {
                comp.call(task);

                fut = comp.future();
            }
            catch (GridException e) {
                // Should not be thrown since uses asynchronous execution.
                fut = new GridFinishedFutureEx<>(e);
            }

            taskFuts.add(fut);

            now = U.currentTimeMillis();
        }

        boolean isInterrupted = false;

        for (IgniteFuture<T> fut : taskFuts) {
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
        for (IgniteFuture<T> fut : taskFuts) {
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
    private void cancelFuture(IgniteFuture<?> fut) {
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

        Collection<IgniteFuture<T>> taskFuts = new ArrayList<>();

        assert comp.isAsync();

        for (Callable<T> cmd : tasks) {
            // Execute task with predefined timeout.
            IgniteFuture<T> fut;

            try
            {
                comp.call(cmd);

                fut = comp.future();
            }
            catch (GridException e) {
                // Should not be thrown since uses asynchronous execution.
                fut = new GridFinishedFutureEx<>(e);
            }

            taskFuts.add(fut);
        }

        T res = null;

        boolean isInterrupted = false;
        boolean isResRcvd = false;

        int errCnt = 0;

        for (IgniteFuture<T> fut : taskFuts) {
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

        assert comp.isAsync();

        try {
            comp.run(cmd);

            addFuture(comp.future());
        }
        catch (GridException e) {
            // Should not be thrown since uses asynchronous execution.
            addFuture(new GridFinishedFutureEx(e));
        }
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
    private <T> Future<T> addFuture(IgniteFuture<T> fut) {
        synchronized (mux) {
            if (!fut.isDone()) {
                fut.listenAsync(lsnr);

                futs.add(fut);
            }

            return new TaskFutureWrapper<>(fut);
        }
    }

    /**
     * Listener to track tasks.
     */
    private class TaskTerminateListener<T> implements IgniteInClosure<IgniteFuture<T>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void apply(IgniteFuture<T> taskFut) {
            synchronized (mux) {
                futs.remove(taskFut);
            }
        }
    }

    /**
     * Wrapper for {@link org.gridgain.grid.IgniteFuture}.
     * Used for compatibility {@link Future} interface.
     * @param <T> The result type of the {@link Future} argument.
     */
    private class TaskFutureWrapper<T> implements Future<T> {
        /** */
        private final IgniteFuture<T> fut;

        /**
         * Creates wrapper.
         *
         * @param fut Grid future.
         */
        TaskFutureWrapper(IgniteFuture<T> fut) {
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
