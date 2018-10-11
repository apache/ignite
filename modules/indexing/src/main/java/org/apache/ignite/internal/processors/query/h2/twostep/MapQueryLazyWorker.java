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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.query.h2.H2ConnectionWrapper;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.ObjectPoolReusable;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.h2.engine.Session;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode.OFF;

/**
 * Worker for lazy query execution.
 */
public class MapQueryLazyWorker extends GridWorker {
    /** Poll task timeout milliseconds. */
    private static final int POLL_TASK_TIMEOUT_MS = 1000;

    /** Lazy thread flag. */
    private static final ThreadLocal<MapQueryLazyWorker> LAZY_WORKER = new ThreadLocal<>();

    /** Active lazy worker count (for testing purposes). */
    private static final LongAdder ACTIVE_CNT = new LongAdder();

    /** Mutex to synchronization worker start/stop. */
    private final Object mux = new Object();

    /** Task to be executed. */
    private final BlockingQueue<Runnable> tasks = new LinkedBlockingDeque<>();

    /** Key. */
    private final MapQueryLazyWorkerKey key;

    /** Map query executor. */
    private final GridMapQueryExecutor exec;

    /** Latch decremented when worker finishes. */
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    /** Query context. */
    private GridH2QueryContext qctx;

    /** Worker is started flag. */
    private boolean started;

    /** Detached connection. */
    private ObjectPoolReusable<H2ConnectionWrapper> detached;

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
     * Start lazy worker for half-processed query.
     * In this case we have to detach H2 connection from current thread and use it for current query processing.
     * Also tables locks must be transferred to lazy thread from QUERY_POOL thread pool.
     *
     * @param ses H2 Session.
     * @param detached H2 connection detached from current thread.
     * @throws QueryCancelledException  In case query is canceled during the worker start.
     */
    void start(Session ses, ObjectPoolReusable<H2ConnectionWrapper> detached) throws QueryCancelledException {
        synchronized (mux) {
            if (!exec.busyLock().enterBusy()) {
                log.warning("Lazy worker isn't started. Node is stopped [key=" + key + ']');

                return;
            }

            try {
                if (started)
                    return;

                if (isCancelled) {
                    if (detached != null)
                        detached.recycle();

                    throw new QueryCancelledException();
                }

                if (ses != null)
                    lazyTransferStart(ses);

                this.detached = detached;

                exec.registerLazyWorker(this);

                IgniteThread thread = new IgniteThread(this);

                started = true;

                thread.start();
            }
            finally {
                exec.busyLock().leaveBusy();
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        LAZY_WORKER.set(this);

        ACTIVE_CNT.increment();

        boolean lockBusy = false;

        try {
            if (qctx != null)
                GridH2QueryContext.set(qctx);

            if(detached != null)
                lazyTransferFinish(H2Utils.session(detached.object().connection()));

            while (!isCancelled()) {
                Runnable task = tasks.poll(POLL_TASK_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                if (task != null) {
                    try {
                        task.run();
                    }
                    catch (Throwable t) {
                        log.warning("Lazy task error", t);
                    }
                }
                else {
                    try {
                        lockBusy = false;

                        if (!exec.busyLock().enterBusy()) {
                            log.info("Stop lazy worker [key=" + key + ']');

                            return;
                        }

                        lockBusy = true;
                    }
                    finally {
                        if (lockBusy)
                            exec.busyLock().leaveBusy();
                    }
                }
            }
        }
        finally {
            exec.unregisterLazyWorker(this);

            LAZY_WORKER.set(null);

            ACTIVE_CNT.decrement();

            stopLatch.countDown();
        }
    }

    /**
     * Submit task to worker.
     *
     * @param task Task to be executed.
     */
    public void submit(Runnable task) {
        if (isCancelled)
            return;

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
     * @param nodeStop Node is stopping.
     */
    private void stop0(boolean nodeStop) {
        synchronized (mux) {
            if (qctx != null && qctx.distributedJoinMode() == OFF && !qctx.isCleared())
                qctx.clearContext(nodeStop);

            if (detached != null) {
                detached.recycle();

                detached = null;
            }

            isCancelled = true;

            mux.notifyAll();
        }
    }

    /**
     * @param task Stop task.
     */
    public void submitStopTask(Runnable task) {
        synchronized (mux) {
            if (LAZY_WORKER.get() != null)
                task.run();
            else
                submit(task);
        }
    }

    /**
     * Stop the worker.
     * @param nodeStop Node is stopping.
     */
    public void stop(final boolean nodeStop) {
        synchronized (mux) {
            if (isCancelled)
                return;

            if (started && currentWorker() == null) {
                submit(new Runnable() {
                    @Override public void run() {
                        stop0(nodeStop);
                    }
                });

                awaitStop();
            }
            else if (currentWorker() != null)
                stop0(nodeStop);
        }
    }

    /**
     * Await worker stop.
     */
    private void awaitStop() {
        synchronized (mux) {
            try {
                if (!isCancelled)
                    mux.wait();

                U.await(stopLatch);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteException("Failed to wait for lazy worker stop (interrupted): " + name(), e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
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
     * Start session transfer to lazy thread.
     *
     * @param ses Session.
     */
    private static void lazyTransferStart(Session ses) {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        assert qctx != null;

        for(GridH2Table tbl : qctx.lockedTables())
            tbl.onLazyTransferStarted(ses);
    }

    /**
     * Finish session transfer to lazy thread.
     *
     * @param ses Session.
     */
    private static void lazyTransferFinish(Session ses) {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        assert qctx != null;

        for(GridH2Table tbl : qctx.lockedTables())
            tbl.onLazyTransferFinished(ses);
    }
}
