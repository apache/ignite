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

package org.apache.ignite.internal.util.worker;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Extension to standard {@link Runnable} interface. Adds proper details to be used
 * with {@link Executor} implementations. Only for internal use.
 */
public abstract class GridWorker implements Runnable {
    /** Ignite logger. */
    protected final IgniteLogger log;

    /** Thread name. */
    private final String name;

    /** */
    private final String gridName;

    /** */
    private final GridWorkerListener lsnr;

    /** */
    private volatile boolean finished;

    /** Whether or not this runnable is cancelled. */
    private volatile boolean isCancelled;

    /** Actual thread runner. */
    private volatile Thread runner;

    /** */
    private final Object mux = new Object();

    /**
     * Creates new grid worker with given parameters.
     *
     * @param gridName Name of grid this runnable is used in.
     * @param name Worker name. Note that in general thread name and worker (runnable) name are two
     *      different things. The same worker can be executed by multiple threads and therefore
     *      for logging and debugging purposes we separate the two.
     * @param log Grid logger to be used.
     * @param lsnr Listener for life-cycle events.
     */
    protected GridWorker(String gridName, String name, IgniteLogger log, @Nullable GridWorkerListener lsnr) {
        assert name != null;
        assert log != null;

        this.gridName = gridName;
        this.name = name;
        this.lsnr = lsnr;
        this.log = log;
    }

    /**
     * Creates new grid worker with given parameters.
     *
     * @param gridName Name of grid this runnable is used in.
     * @param name Worker name. Note that in general thread name and worker (runnable) name are two
     *      different things. The same worker can be executed by multiple threads and therefore
     *      for logging and debugging purposes we separate the two.
     * @param log Grid logger to be used.
     */
    protected GridWorker(@Nullable String gridName, String name, IgniteLogger log) {
        this(gridName, name, log, null);
    }

    /** {@inheritDoc} */
    @Override public final void run() {
        // Runner thread must be recorded first as other operations
        // may depend on it being present.
        runner = Thread.currentThread();

        if (log.isDebugEnabled())
            log.debug("Grid runnable started: " + name);

        try {
            // Special case, when task gets cancelled before it got scheduled.
            if (isCancelled)
                runner.interrupt();

            // Listener callback.
            if (lsnr != null)
                lsnr.onStarted(this);

            body();
        }
        catch (IgniteInterruptedCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Caught interrupted exception: " + e);
        }
        catch (InterruptedException e) {
            if (log.isDebugEnabled())
                log.debug("Caught interrupted exception: " + e);

            Thread.currentThread().interrupt();
        }
        // Catch everything to make sure that it gets logged properly and
        // not to kill any threads from the underlying thread pool.
        catch (Throwable e) {
            if (!X.hasCause(e, InterruptedException.class) && !X.hasCause(e, IgniteInterruptedCheckedException.class) && !X.hasCause(e, IgniteInterruptedException.class))
                U.error(log, "Runtime error caught during grid runnable execution: " + this, e);
            else
                U.warn(log, "Runtime exception occurred during grid runnable execution caused by thread interruption: " + e.getMessage());

            if (e instanceof Error)
                throw e;
        }
        finally {
            synchronized (mux) {
                finished = true;

                mux.notifyAll();
            }

            cleanup();

            if (lsnr != null)
                lsnr.onStopped(this);

            if (log.isDebugEnabled())
                if (isCancelled)
                    log.debug("Grid runnable finished due to cancellation: " + name);
                else if (runner.isInterrupted())
                    log.debug("Grid runnable finished due to interruption without cancellation: " + name);
                else
                    log.debug("Grid runnable finished normally: " + name);

            // Need to set runner to null, to make sure that
            // further operations on this runnable won't
            // affect the thread which could have been recycled
            // by thread pool.
            runner = null;
        }
    }

    /**
     * The implementation should provide the execution body for this runnable.
     *
     * @throws InterruptedException Thrown in case of interruption.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    protected abstract void body() throws InterruptedException, IgniteInterruptedCheckedException;

    /**
     * Optional method that will be called after runnable is finished. Default
     * implementation is no-op.
     */
    protected void cleanup() {
        /* No-op. */
    }

    /**
     * @return Runner thread.
     */
    public Thread runner() {
        return runner;
    }

    /**
     * Gets name of the grid this runnable belongs to.
     *
     * @return Name of the grid this runnable belongs to.
     */
    public String gridName() {
        return gridName;
    }

    /**
     * Gets this runnable name.
     *
     * @return This runnable name.
     */
    public String name() {
        return name;
    }

    /**
     * Cancels this runnable interrupting actual runner.
     */
    public void cancel() {
        if (log.isDebugEnabled())
            log.debug("Cancelling grid runnable: " + this);

        isCancelled = true;

        Thread runner = this.runner;

        // Cannot apply Future.cancel() because if we do, then Future.get() would always
        // throw CancellationException and we would not be able to wait for task completion.
        if (runner != null)
            runner.interrupt();
    }

    /**
     * Joins this runnable.
     *
     * @throws InterruptedException Thrown in case of interruption.
     */
    public void join() throws InterruptedException {
        if (log.isDebugEnabled())
            log.debug("Joining grid runnable: " + this);

        if ((runner == null && isCancelled) || finished)
            return;

        synchronized (mux) {
            while (!finished)
                mux.wait();
        }
    }

    /**
     * Tests whether or not this runnable is cancelled.
     *
     * @return {@code true} if this runnable is cancelled - {@code false} otherwise.
     * @see Future#isCancelled()
     */
    public boolean isCancelled() {
        Thread runner = this.runner;

        return isCancelled || (runner != null && runner.isInterrupted());
    }

    /**
     * Tests whether or not this runnable is finished.
     *
     * @return {@code true} if this runnable is finished - {@code false} otherwise.
     */
    public boolean isDone() {
        return finished;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Thread runner = this.runner;

        return S.toString(GridWorker.class, this,
            "hashCode", hashCode(),
            "interrupted", (runner != null ? runner.isInterrupted() : "unknown"),
            "runner", (runner == null ? "null" : runner.getName()));
    }
}