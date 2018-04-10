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

package org.apache.ignite.internal.util;

import java.util.concurrent.BlockingQueue;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

/**
 * Wrapped striped executor which delegates all jobs to target executor service.
 * It is used instead of the striped executor when that one is disabled (striped pool size <= 0).
 */
public class StripedExecutorProxy extends IgniteThreadPoolExecutor implements StripedExecutor {
    /**
     * Creates a new service with the given initial parameters.
     *
     * @param threadNamePrefix Will be added at the beginning of all created threads.
     * @param igniteInstanceName Must be the name of the grid.
     * @param corePoolSize The number of threads to keep in the pool, even if they are idle.
     * @param maxPoolSize The maximum number of threads to allow in the pool.
     * @param keepAliveTime When the number of threads is greater than the core, this is the maximum time
     *      that excess idle threads will wait for new tasks before terminating.
     * @param workQ The queue to use for holding tasks before they are executed. This queue will hold only
     *      runnable tasks submitted by the {@link #execute(Runnable)} method.
     * @param plc {@link GridIoPolicy} for thread pool.
     */
    public StripedExecutorProxy(
        String threadNamePrefix,
        String igniteInstanceName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        byte plc) {
        super(
            threadNamePrefix,
            igniteInstanceName,
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            workQ,
            plc
        );
    }

    /** {@inheritDoc} */
    @Override public void checkStarvation() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int stripes() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void execute(int idx, Runnable cmd) {
        execute(cmd);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int queueSize() {
        return getQueue().size();
    }

    /** {@inheritDoc} */
    @Override public long completedTasks() {
        return getCompletedTaskCount();
    }

    /** {@inheritDoc} */
    @Override public long[] stripesCompletedTasks() {
        return new long[] {completedTasks()};
    }

    /** {@inheritDoc} */
    @Override public boolean[] stripesActiveStatuses() {
        return new boolean[] {getActiveCount() > 0};
    }

    /** {@inheritDoc} */
    @Override public int activeStripesCount() {
        return getActiveCount() > 0 ? 1 : 0;
    }

    /** {@inheritDoc} */
    @Override public int[] stripesQueueSizes() {
        return new int[] {queueSize()};
    }
}
