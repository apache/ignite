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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.jetbrains.annotations.Nullable;
import org.jsr166.LongAdder8;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Worker for lazy query execution.
 */
public class MapQueryLazyWorker extends GridWorker {
    /** Lazy thread flag. */
    private static final ThreadLocal<MapQueryLazyWorker> LAZY_WORKER = new ThreadLocal<>();

    /** Active lazy worker count (for testing purposes). */
    private static final LongAdder8 ACTIVE_CNT = new LongAdder8();

    /** Task to be executed. */
    private final BlockingQueue<Runnable> tasks = new LinkedBlockingDeque<>();

    /** Key. */
    private final MapQueryLazyWorkerKey key;

    /** Map query executor. */
    private final GridMapQueryExecutor exec;

    /** Latch decremented when worker finishes. */
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    /** Map query result. */
    private volatile MapQueryResult res;

    /**
     * Constructor.
     *
     * @param instanceName Instance name.
     * @param key Lazy worker key.
     * @param log Logger.
     * @param exec Map query executor.
     */
    public MapQueryLazyWorker(@Nullable String instanceName, MapQueryLazyWorkerKey key, IgniteLogger log,
        GridMapQueryExecutor exec) {
        super(instanceName, workerName(instanceName, key), log);

        this.key = key;
        this.exec = exec;
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        LAZY_WORKER.set(this);

        ACTIVE_CNT.increment();

        try {
            while (!isCancelled()) {
                Runnable task = tasks.take();

                if (task != null)
                    task.run();
            }
        }
        finally {
            if (res != null)
                res.close();

            LAZY_WORKER.set(null);

            ACTIVE_CNT.decrement();

            exec.unregisterLazyWorker(this);
        }
    }

    /**
     * Submit task to worker.
     *
     * @param task Task to be executed.
     */
    public void submit(Runnable task) {
        tasks.add(task);
    }

    /**
     * @return Worker key.
     */
    public MapQueryLazyWorkerKey key() {
        return key;
    }

    /**
     * Stop the worker.
     */
    public void stop() {
        if (MapQueryLazyWorker.currentWorker() == null)
            submit(new Runnable() {
                @Override public void run() {
                    stop();
                }
            });
        else {
            isCancelled = true;

            stopLatch.countDown();
        }
    }

    /**
     * Await worker stop.
     */
    public void awaitStop() {
        try {
            U.await(stopLatch);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteException("Failed to wait for lazy worker stop (interrupted): " + name(), e);
        }
    }

    /**
     * @param res Map query result.
     */
    public void result(MapQueryResult res) {
        this.res = res;
    }

    /**
     * @return Current worker or {@code null} if call is performed not from lazy worker thread.
     */
    @Nullable public static MapQueryLazyWorker currentWorker() {
        return LAZY_WORKER.get();
    }

    /**
     * @return Active workers count.
     */
    public static int activeCount() {
        return ACTIVE_CNT.intValue();
    }

    /**
     * Construct worker name.
     *
     * @param instanceName Instance name.
     * @param key Key.
     * @return Name.
     */
    private static String workerName(String instanceName, MapQueryLazyWorkerKey key) {
        return "query-lazy-worker_" + instanceName + "_" + key.nodeId() + "_" + key.queryRequestId() + "_" +
            key.segment();
    }
}
