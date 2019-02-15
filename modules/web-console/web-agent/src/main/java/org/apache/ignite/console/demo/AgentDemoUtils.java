/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.console.demo;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utilities for Agent demo mode.
 */
public class AgentDemoUtils {
    /** Counter for threads in pool. */
    private static final AtomicInteger THREAD_CNT = new AtomicInteger(0);

    /**
     * Creates a thread pool that can schedule commands to run after a given delay, or to execute periodically.
     *
     * @param corePoolSize Number of threads to keep in the pool, even if they are idle.
     * @param threadName Part of thread name that would be used by thread factory.
     * @return Newly created scheduled thread pool.
     */
    public static ScheduledExecutorService newScheduledThreadPool(int corePoolSize, final String threadName) {
        ScheduledExecutorService srvc = Executors.newScheduledThreadPool(corePoolSize, new ThreadFactory() {
            @Override public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, String.format("%s-%d", threadName, THREAD_CNT.getAndIncrement()));

                thread.setDaemon(true);

                return thread;
            }
        });

        ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor)srvc;

        // Setting up shutdown policy.
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        return srvc;
    }

    /**
     * Round value.
     *
     * @param val Value to round.
     * @param places Numbers after point.
     * @return Rounded value;
     */
    public static double round(double val, int places) {
        if (places < 0)
            throw new IllegalArgumentException();

        long factor = (long)Math.pow(10, places);

        val *= factor;

        long tmp = Math.round(val);

        return (double)tmp / factor;
    }
}
