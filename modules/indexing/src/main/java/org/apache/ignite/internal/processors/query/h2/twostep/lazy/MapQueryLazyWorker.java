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

package org.apache.ignite.internal.processors.query.h2.twostep.lazy;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Worker for lazy query execution.
 */
public class MapQueryLazyWorker extends GridWorker {
    /** Lazy thread flag. */
    private static final ThreadLocal<MapQueryLazyWorker> LAZY_WORKER = new ThreadLocal<>();

    /** Task to be executed. */
    private final BlockingQueue<Runnable> tasks = new LinkedBlockingDeque<>();

    /**
     * Constructor.
     *
     * @param instanceName Instance name.
     * @param key Lazy worker key.
     * @param log Logger.
     */
    public MapQueryLazyWorker(@Nullable String instanceName, MapQueryLazyWorkerKey key, IgniteLogger log) {
        super(instanceName, workerName(key), log);
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        LAZY_WORKER.set(this);

        try {
            while (!isCancelled()) {
                Runnable task = tasks.poll();

                if (task != null)
                    task.run();
            }
        }
        finally {
            LAZY_WORKER.set(null);
        }
    }

    /**
     * Stop the worker.
     */
    public void stop() {
        tasks.add(new StopTask());
    }

    /**
     * Internal worker stop routine.
     */
    private void stop0() {
        isCancelled = true;
    }

    /**
     * @return Lazy worker thread flag.
     */
    public static boolean isLazyWorkerThread() {
        MapQueryLazyWorker worker = LAZY_WORKER.get();

        return worker != null;
    }

    /**
     * Construct worker name.
     *
     * @param key Key.
     * @return Name.
     */
    private static String workerName(MapQueryLazyWorkerKey key) {
        return "query-lazy-worker_" + key.nodeId() + "_" + key.queryRequestId() + "_" + key.segment();
    }

    /**
     * Internal stop task.
     */
    private static class StopTask implements Runnable {
        @Override public void run() {
            MapQueryLazyWorker worker = LAZY_WORKER.get();

            assert worker != null;

            worker.stop0();
        }
    }
}
