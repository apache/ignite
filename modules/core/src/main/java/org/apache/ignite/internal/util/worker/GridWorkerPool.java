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

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Pool of runnable workers. This class automatically takes care of
 * error handling that has to do with executing a runnable task and
 * ensures that all tasks are finished when stop occurs.
 */
public class GridWorkerPool {
    /** */
    private final Executor exec;

    /** */
    private final IgniteLogger log;

    /** */
    private final Collection<GridWorker> workers = new GridConcurrentHashSet<>();

    /**
     * @param exec Executor service.
     * @param log Logger to use.
     */
    public GridWorkerPool(Executor exec, IgniteLogger log) {
        assert exec != null;
        assert log != null;

        this.exec = exec;
        this.log = log;
    }

    /**
     * Schedules runnable task for execution.
     *
     * @param w Runnable task.
     * @throws IgniteCheckedException Thrown if any exception occurred.
     */
    @SuppressWarnings({"CatchGenericClass", "ProhibitedExceptionThrown"})
    public void execute(final GridWorker w) throws IgniteCheckedException {
        workers.add(w);

        try {
            exec.execute(new Runnable() {
                @Override public void run() {
                    try {
                        w.run();
                    }
                    finally {
                        workers.remove(w);
                    }
                }
            });
        }
        catch (RejectedExecutionException e) {
            workers.remove(w);

            throw new ComputeExecutionRejectedException("Failed to execute worker due to execution rejection.", e);
        }
        catch (RuntimeException e) {
            workers.remove(w);

            throw new IgniteCheckedException("Failed to execute worker due to runtime exception.", e);
        }
        catch (Error e) {
            workers.remove(w);

            throw e;
        }
    }

    /**
     * Waits for all workers to finish.
     *
     * @param cancel Flag to indicate whether workers should be cancelled
     *      before waiting for them to finish.
     */
    public void join(boolean cancel) {
        if (cancel)
            U.cancel(workers);

        // Record current interrupted status of calling thread.
        boolean interrupted = Thread.interrupted();

        try {
            U.join(workers, log);
        }
        finally {
            // Reset interrupted flag on calling thread.
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }
}