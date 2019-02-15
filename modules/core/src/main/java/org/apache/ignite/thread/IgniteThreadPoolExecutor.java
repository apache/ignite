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

package org.apache.ignite.thread;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;

/**
 * An {@link ExecutorService} that executes submitted tasks using pooled grid threads.
 */
public class IgniteThreadPoolExecutor extends ThreadPoolExecutor {
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
     */
    public IgniteThreadPoolExecutor(
        String threadNamePrefix,
        String igniteInstanceName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ) {
        this(threadNamePrefix,
            igniteInstanceName,
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            workQ,
            GridIoPolicy.UNDEFINED,
            null);
    }

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
     * @param eHnd Uncaught exception handler for thread pool.
     */
    public IgniteThreadPoolExecutor(
        String threadNamePrefix,
        String igniteInstanceName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        byte plc,
        UncaughtExceptionHandler eHnd) {
        super(
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            TimeUnit.MILLISECONDS,
            workQ,
            new IgniteThreadFactory(igniteInstanceName, threadNamePrefix, plc, eHnd)
        );
    }

    /**
     * Creates a new service with the given initial parameters.
     *
     * @param corePoolSize The number of threads to keep in the pool, even if they are idle.
     * @param maxPoolSize The maximum number of threads to allow in the pool.
     * @param keepAliveTime When the number of threads is greater than the core, this is the maximum time
     *      that excess idle threads will wait for new tasks before terminating.
     * @param workQ The queue to use for holding tasks before they are executed. This queue will hold only the
     *      runnable tasks submitted by the {@link #execute(Runnable)} method.
     * @param threadFactory Thread factory.
     */
    public IgniteThreadPoolExecutor(
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        ThreadFactory threadFactory) {
        super(
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            TimeUnit.MILLISECONDS,
            workQ,
            threadFactory,
            new AbortPolicy()
        );
    }
}
