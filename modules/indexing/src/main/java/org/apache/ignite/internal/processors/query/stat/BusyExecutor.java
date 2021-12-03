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

package org.apache.ignite.internal.processors.query.stat;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

/**
 * Executor with busy run support.
 * Can run any tasks while active and safelly wait untill they stopped.
 */
public class BusyExecutor {
    /** Logger. */
    private final IgniteLogger log;

    /** Executor name. */
    private final String name;

    /** Active flag (used to skip commands in inactive cluster.) */
    private volatile boolean active;

    /** Lock protection of started gathering during deactivation. */
    private volatile GridBusyLock busyLock = new GridBusyLock();

    /** Executor pool. */
    private final IgniteThreadPoolExecutor pool;

    /** Cancellable tasks. */
    private final GridConcurrentHashSet<CancellableTask> cancellableTasks = new GridConcurrentHashSet<>();

    /** External stopping supplier. */
    Supplier<Boolean> stopping;

    /**
     * Constructor.
     *
     * @param name Executor name.
     * @param pool Underlying thread pool executor.
     * @param stopping External stopping state supplier.
     * @param logSupplier Log supplier.
     */
    public BusyExecutor(
        String name,
        IgniteThreadPoolExecutor pool,
        Supplier<Boolean> stopping,
        Function<Class<?>, IgniteLogger> logSupplier) {
        this.name = name;
        this.pool = pool;
        this.stopping = stopping;
        this.log = logSupplier.apply(StatisticsProcessor.class);
        busyLock.block();
    }

    /**
     * Allow operations.
     */
    public synchronized void activate() {
        busyLock = new GridBusyLock();

        active = true;

        if (log.isDebugEnabled())
            log.debug("Busy executor " + name + " activated.");
    }

    /**
     * Stop all running tasks. Block new task scheduling, execute cancell runnable and wait till each task stops.
     */
    public synchronized void deactivate() {
        if (!active)
            return;

        active = false;

        if (log.isDebugEnabled())
            log.debug("Busy executor " + name + " deactivating.");

        cancellableTasks.forEach(CancellableTask::cancel);

        busyLock.block();

        if (log.isDebugEnabled())
            log.debug("Busy executor " + name + " deactivated.");
    }

    /**
     * Run task on busy lock.
     *
     * @param r Task to run.
     * @return {@code true} if task was succesfully scheduled, {@code false} - otherwise (due to inactive state).
     */
    public boolean busyRun(Runnable r) {
        return busyRun(r, busyLock);
    }

    /**
     * Run task under specified busyLock.
     *
     * @param r Task to run.
     * @param taskLock BusyLock to use.
     * @return {@code true} if task was succesfully scheduled, {@code false} - otherwise (due to inactive state).
     */
    private boolean busyRun(Runnable r, GridBusyLock taskLock) {
        if (!taskLock.enterBusy())
            return false;

        try {
            if (!active)
                return false;

            r.run();

            return true;
        }
        catch (Throwable t) {
            if (stopping.get())
                log.debug("Unexpected exception on statistics processing: " + t);
            else
                log.warning("Unexpected exception on statistics processing: " + t.getMessage(), t);
        }
        finally {
            taskLock.leaveBusy();
        }

        return false;
    }

    /**
     * Submit task to execute in thread pool under busy lock.
     * Task surrounded with try/catch and if it's complete with any exception - resulting future will return
     *
     * @param r Task to execute.
     * @return Completable future with executed flag in result.
     */
    public CompletableFuture<Boolean> submit(Runnable r) {
        GridBusyLock lock = busyLock;

        CompletableFuture<Boolean> res = new CompletableFuture<>();

        if (r instanceof CancellableTask) {
            CancellableTask ct = (CancellableTask)r;

            res.thenApply(result -> {
                cancellableTasks.remove(ct);

                return result;
            });

            cancellableTasks.add(ct);
        }

        pool.execute(() -> res.complete(busyRun(r, lock)));

        return res;
    }

    /**
     * Execute task in thread pool under busy lock.
     *
     * @param r Task to execute.
     */
    public void execute(Runnable r) {
        submit(r);
    }

    /**
     * Execute cancellable task in thread pool under busy lock. Track task to cancel on executor stop.
     *
     * @param ct Cancellable task to execute.
     */
    public void execute(CancellableTask ct) {
        GridBusyLock lock = busyLock;

        cancellableTasks.add(ct);

        pool.execute(() -> {
            busyRun(ct, lock);

            cancellableTasks.remove(ct);
        });
    }
}
