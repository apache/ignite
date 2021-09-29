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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridBusyLock;
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
    private static final GridBusyLock busyLock = new GridBusyLock();

    /** Executor pool. */
    private final IgniteThreadPoolExecutor pool;

    /**
     * Constructor.
     *
     * @param name Executor name.
     * @param pool Underlying thread pool executor.
     * @param logSupplier Log supplier.
     */
    public BusyExecutor(String name, IgniteThreadPoolExecutor pool, Function<Class<?>, IgniteLogger> logSupplier) {
        this.name = name;
        this.pool = pool;
        this.log = logSupplier.apply(StatisticsProcessor.class);
    }

    /**
     * Allow operations.
     */
    public void activate() {
        active = true;

        if (log.isDebugEnabled())
            log.debug("Busy executor " + name + " activated.");
    }

    /**
     * Stop all running tasks. Block new task scheduling, execute cancell runnable and wait till each task stops.
     *
     * @param r Runnable to cancel all scheduled tasks.
     */
    public void deactivate(Runnable r) {
        active = false;

        if (log.isDebugEnabled())
            log.debug("Busy executor " + name + " deactivating.");

        r.run();

        busyLock.block();
        busyLock.unblock();

        if (log.isDebugEnabled())
            log.debug("Busy executor " + name + " deactivated.");
    }

    /**
     * Run task on busy lock.
     *
     * @param r Task to run.
     * @return {@code true} if task was succesfully scheduled, {@code false} - otherwise (due to inactive state)
     */
    public boolean busyRun(Runnable r) {
        if (!busyLock.enterBusy())
            return false;

        try {
            if (!active)
                return false;

            r.run();
        }
        catch (Throwable t) {
            log.warning("Unexpected exception on statistics processing: " + t.getMessage(), t);
        }
        finally {
            busyLock.leaveBusy();
        }

        return true;
    }

    /**
     * Submit task to execute in thread pool. Task surrounded with try/catch and if it's complete with any exception -
     * resulting future will return false.
     *
     * @param r Task to execute.
     * @return Completable future.
     */
    public CompletableFuture<Boolean> submit(Runnable r) {
        CompletableFuture<Boolean> res = new CompletableFuture<>();

        pool.execute(() -> res.complete(busyRun(r)));

        return res;
    }
}
