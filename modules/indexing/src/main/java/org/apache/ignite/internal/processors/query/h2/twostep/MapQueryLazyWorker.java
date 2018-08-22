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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.query.h2.H2ConnectionWrapper;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.ObjectPool;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

/**
 * Worker for lazy query execution.
 */
public class MapQueryLazyWorker extends GridWorker {
    /** Lazy thread flag. */
    private static final ThreadLocal<MapQueryLazyWorker> LAZY_WORKER = new ThreadLocal<>();

    /** Active lazy worker count (for testing purposes). */
    private static final LongAdder ACTIVE_CNT = new LongAdder();

    /** Task to be executed. */
    private final BlockingQueue<Runnable> tasks = new LinkedBlockingDeque<>();

    /** Key. */
    private final MapQueryLazyWorkerKey key;

    /** Map query executor. */
    private final GridMapQueryExecutor exec;

    /** Latch decremented when worker finishes. */
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    /** Latch decremented when worker finishes. */
    private GridH2QueryContext qctx;

    /** Map query result. */
    private volatile MapQueryResult res;

    /** Worker is started. */
    private volatile boolean started;

    /** Worker is stopped. */
    private volatile boolean stopped;

    /** Detached connection. */
    private ObjectPool.Reusable<H2ConnectionWrapper> detached;

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

    /**
     *
     */
    void start() {
        if (started)
            return;

        started = true;

        IgniteThread thread = new IgniteThread(this);

        thread.start();
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        LAZY_WORKER.set(this);

        ACTIVE_CNT.increment();

        try {
            if (qctx != null)
                GridH2QueryContext.set(qctx);

            if(detached != null)
                GridH2Table.attachReadLocksToCurrentThread(H2Utils.session(detached.object().connection()));

            while (!isCancelled()) {
                Runnable task = tasks.take();

                if (task != null) {
                    if (!exec.busyLock().enterBusy())
                        return;

                    try {
                        task.run();
                    }
                    finally {
                        exec.busyLock().leaveBusy();
                    }
                }
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
     * @param task Statement cancel task.
     */
    public void runStatementCancelTask(Runnable task) {
        if (isStarted())
            submit(task);
        else
            task.run();
    }

    /**
     * @return Worker key.
     */
    public MapQueryLazyWorkerKey key() {
        return key;
    }

    /**
     * Stop the worker.
     * @param nodeStop Node is stopping.
     */
    public void stop(final boolean nodeStop) {
        if (stopped)
            return;

        if (started && currentWorker() == null)
            submit(new Runnable() {
                @Override public void run() {
                    stop(nodeStop);
                }
            });
        else {
            if (stopped)
                return;

            GridH2QueryContext qctx = GridH2QueryContext.get();

            if (qctx != null) {
                qctx.clearContext(nodeStop);

                GridH2QueryContext.clearThreadLocal();
            }

            isCancelled = true;

            stopLatch.countDown();

            if (detached != null)
                detached.recycle();

            stopped = true;
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
     * @param qctx Query context.
     */
    public void queryContext(GridH2QueryContext qctx) {
        this.qctx = qctx;
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

    /**
     * @param conn Detached H2 connection.
     */
    public void detachedConnection(ObjectPool.Reusable<H2ConnectionWrapper> conn) {
        this.detached = conn;
    }

    /**
     * @return {@code true} if the worker have started.
     */
    public boolean isStarted() {
        return started;
    }
}
