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

package org.apache.ignite.internal.processors.hadoop.taskexecutor;


import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerListener;
import org.apache.ignite.internal.util.worker.GridWorkerListenerAdapter;
import org.apache.ignite.thread.IgniteThread;
import org.jsr166.ConcurrentHashMap8;

import static java.util.Collections.newSetFromMap;

/**
 * Executor service without thread pooling.
 */
public class HadoopExecutorService {
    /** */
    private final LinkedBlockingQueue<Callable<?>> queue;

    /** */
    private final Collection<GridWorker> workers = newSetFromMap(new ConcurrentHashMap8<GridWorker, Boolean>());

    /** */
    private final AtomicInteger active = new AtomicInteger();

    /** */
    private final int maxTasks;

    /** */
    private final String gridName;

    /** */
    private final IgniteLogger log;

    /** */
    private volatile boolean shutdown;

    /** */
    private final GridWorkerListener lsnr = new GridWorkerListenerAdapter() {
            @Override public void onStopped(GridWorker w) {
                workers.remove(w);

                if (shutdown) {
                    active.decrementAndGet();

                    return;
                }

                Callable<?> task = queue.poll();

                if (task != null)
                    startThread(task);
                else {
                    active.decrementAndGet();

                    if (!queue.isEmpty())
                        startFromQueue();
                }
            }
        };

    /**
     * @param log Logger.
     * @param gridName Grid name.
     * @param maxTasks Max number of tasks.
     * @param maxQueue Max queue length.
     */
    public HadoopExecutorService(IgniteLogger log, String gridName, int maxTasks, int maxQueue) {
        assert maxTasks > 0 : maxTasks;
        assert maxQueue > 0 : maxQueue;

        this.maxTasks = maxTasks;
        this.queue = new LinkedBlockingQueue<>(maxQueue);
        this.gridName = gridName;
        this.log = log.getLogger(HadoopExecutorService.class);
    }

    /**
     * @return Number of active workers.
     */
    public int active() {
        return workers.size();
    }

    /**
     * Submit task.
     *
     * @param task Task.
     */
    public void submit(Callable<?> task) {
        while (queue.isEmpty()) {
            int active0 = active.get();

            if (active0 == maxTasks)
                break;

            if (active.compareAndSet(active0, active0 + 1)) {
                startThread(task);

                return; // Started in new thread bypassing queue.
            }
        }

        try {
            while (!queue.offer(task, 100, TimeUnit.MILLISECONDS)) {
                if (shutdown)
                    return; // Rejected due to shutdown.
            }
        }
        catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();

            return;
        }

        startFromQueue();
    }

    /**
     * Attempts to start task from queue.
     */
    private void startFromQueue() {
        do {
            int active0 = active.get();

            if (active0 == maxTasks)
                break;

            if (active.compareAndSet(active0, active0 + 1)) {
                Callable<?> task = queue.poll();

                if (task == null) {
                    int res = active.decrementAndGet();

                    assert res >= 0 : res;

                    break;
                }

                startThread(task);
            }
        }
        while (!queue.isEmpty());
    }

    /**
     * @param task Task.
     */
    private void startThread(final Callable<?> task) {
        String workerName;

        if (task instanceof HadoopRunnableTask) {
            final HadoopTaskInfo i = ((HadoopRunnableTask)task).taskInfo();

            workerName = "Hadoop-task-" + i.jobId() + "-" + i.type() + "-" + i.taskNumber() + "-" + i.attempt();
        }
        else
            workerName = task.toString();

        GridWorker w = new GridWorker(gridName, workerName, log, lsnr) {
            @Override protected void body() {
                try {
                    task.call();
                }
                catch (Exception e) {
                    log.error("Failed to execute task: " + task, e);
                }
            }
        };

        workers.add(w);

        if (shutdown)
            w.cancel();

        new IgniteThread(w).start();
    }

    /**
     * Shuts down this executor service.
     *
     * @param awaitTimeMillis Time in milliseconds to wait for tasks completion.
     * @return {@code true} If all tasks completed.
     */
    public boolean shutdown(long awaitTimeMillis) {
        shutdown = true;

        for (GridWorker w : workers)
            w.cancel();

        while (awaitTimeMillis > 0 && !workers.isEmpty()) {
            try {
                Thread.sleep(100);

                awaitTimeMillis -= 100;
            }
            catch (InterruptedException ignored) {
                break;
            }
        }

        return workers.isEmpty();
    }

    /**
     * @return {@code true} If method {@linkplain #shutdown(long)} was already called.
     */
    public boolean isShutdown() {
        return shutdown;
    }
}
