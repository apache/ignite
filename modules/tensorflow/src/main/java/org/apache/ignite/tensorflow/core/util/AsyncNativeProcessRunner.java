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

package org.apache.ignite.tensorflow.core.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;

/**
 * Asynchronous native process runner.
 */
public abstract class AsyncNativeProcessRunner {
    /** Ignite logger. */
    private final IgniteLogger log;

    /** Executors that is used to start async native process. */
    private final ExecutorService executor;

    /** Future of the async process process. */
    private Future<?> fut;

    /**
     * Constructs a new asynchronous native process runner.
     *
     * @param ignite Ignite instance.
     * @param executor Executor.
     */
    public AsyncNativeProcessRunner(Ignite ignite, ExecutorService executor) {
        this.log = ignite.log().getLogger(AsyncNativeProcessRunner.class);
        this.executor = executor;
    }

    /**
     * Method that should be called before starting the process.
     *
     * @return Prepared native process runner.
     */
    public abstract NativeProcessRunner doBefore();

    /**
     * Method that should be called after starting the process.
     */
    public abstract void doAfter();

    /**
     * Starts the process in separate thread.
     */
    public synchronized void start() {
        if (fut != null)
            throw new IllegalStateException("Async native process has already been started");

        NativeProcessRunner procRunner = doBefore();

        fut = executor.submit(() -> {
            try {
                log.debug("Starting native process");
                procRunner.startAndWait();
                log.debug("Native process completed");
            }
            catch (InterruptedException e) {
                log.debug("Native process interrupted");
            }
            catch (Exception e) {
                log.error("Native process failed", e);
                throw e;
            }
            finally {
                doAfter();
            }
        });
    }

    /**
     * Stops the process.
     */
    public synchronized void stop() {
        if (fut != null && !fut.isDone())
            fut.cancel(true);
    }

    /**
     * Checks if process is already completed.
     *
     * @return {@code true} if process completed, otherwise {@code false}.
     */
    public boolean isCompleted() {
        return fut != null && fut.isDone();
    }

    /**
     * Returns an exception that happened during execution or {@code null} if there is no exception.
     *
     * @return Exception that happened during execution or {@code null} if there is no exception.
     */
    public Exception getException() {
        if (!fut.isDone())
            return null;

        try {
            fut.get();
        }
        catch (InterruptedException | ExecutionException e) {
            return e;
        }

        return null;
    }
}
