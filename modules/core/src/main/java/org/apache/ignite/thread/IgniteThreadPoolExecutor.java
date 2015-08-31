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

package org.apache.ignite.thread;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.Nullable;

/**
 * An {@link ExecutorService} that executes submitted tasks using pooled grid threads.
 */
public class IgniteThreadPoolExecutor extends ThreadPoolExecutor {
    /** Default core pool size (value is {@code 100}). */
    public static final int DFLT_CORE_POOL_SIZE = 100;

    /**
     * Creates a new service with default initial parameters.
     * Default values are:
     * <table class="doctable">
     * <tr>
     *      <th>Name</th>
     *      <th>Default Value</th>
     * </tr>
     * <tr>
     *      <td>Core Pool Size</td>
     *      <td>{@code 100} (see {@link #DFLT_CORE_POOL_SIZE}).</td>
     * </tr>
     * <tr>
     *      <td>Maximum Pool Size</td>
     *      <td>None, is it is not used for unbounded queues.</td>
     * </tr>
     * <tr>
     *      <td>Keep alive time</td>
     *      <td>No limit (see {@link Long#MAX_VALUE}).</td>
     * </tr>
     * <tr>
     *      <td>Blocking Queue (see {@link BlockingQueue}).</td>
     *      <td>Unbounded linked blocking queue (see {@link LinkedBlockingDeque}).</td>
     * </tr>
     * </table>
     */
    public IgniteThreadPoolExecutor() {
        this(
            DFLT_CORE_POOL_SIZE,
            DFLT_CORE_POOL_SIZE,
            0,
            new LinkedBlockingDeque<Runnable>(),
            new IgniteThreadFactory(null),
            null
        );
    }

    /**
     * Creates a new service with the given initial parameters.
     *
     * @param corePoolSize The number of threads to keep in the pool, even if they are idle.
     * @param maxPoolSize The maximum number of threads to allow in the pool.
     * @param keepAliveTime When the number of threads is greater than the core, this is the maximum time
     *      that excess idle threads will wait for new tasks before terminating.
     * @param workQueue The queue to use for holding tasks before they are executed. This queue will hold only
     *      runnable tasks submitted by the {@link #execute(Runnable)} method.
     */
    public IgniteThreadPoolExecutor(
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQueue) {
        this(
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            workQueue,
            new IgniteThreadFactory(null),
            null
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
     * @param hnd Optional handler to use when execution is blocked because the thread bounds and queue
     *      capacities are reached. If {@code null} then {@code AbortPolicy}
     *      handler is used by default.
     */
    public IgniteThreadPoolExecutor(
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        RejectedExecutionHandler hnd) {
        this(
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            workQ,
            new IgniteThreadFactory(null),
            hnd
        );
    }

    /**
     * Creates a new service with default initial parameters.
     * Default values are:
     * <table class="doctable">
     * <tr>
     *      <th>Name</th>
     *      <th>Default Value</th>
     * </tr>
     * <tr>
     *      <td>Core Pool Size</td>
     *      <td>{@code 100} (see {@link #DFLT_CORE_POOL_SIZE}).</td>
     * </tr>
     * <tr>
     *      <td>Maximum Pool Size</td>
     *      <td>None, is it is not used for unbounded queues.</td>
     * </tr>
     * <tr>
     *      <td>Keep alive time</td>
     *      <td>No limit (see {@link Long#MAX_VALUE}).</td>
     * </tr>
     * <tr>
     *      <td>Blocking Queue (see {@link BlockingQueue}).</td>
     *      <td>Unbounded linked blocking queue (see {@link LinkedBlockingDeque}).</td>
     * </tr>
     * </table>
     *
     * @param gridName Name of the grid.
     */
    public IgniteThreadPoolExecutor(String gridName) {
        this(
            DFLT_CORE_POOL_SIZE,
            DFLT_CORE_POOL_SIZE,
            0,
            new LinkedBlockingDeque<Runnable>(),
            new IgniteThreadFactory(gridName),
            null
        );
    }

    /**
     * Creates a new service with the given initial parameters.
     *
     * @param gridName Name of the grid
     * @param corePoolSize The number of threads to keep in the pool, even if they are idle.
     * @param maxPoolSize The maximum number of threads to allow in the pool.
     * @param keepAliveTime When the number of threads is greater than the core, this is the maximum time
     *      that excess idle threads will wait for new tasks before terminating.
     * @param workQ The queue to use for holding tasks before they are executed. This queue will hold only
     *      runnable tasks submitted by the {@link #execute(Runnable)} method.
     */
    public IgniteThreadPoolExecutor(
        String gridName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ) {
        super(
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            TimeUnit.MILLISECONDS,
            workQ,
            new IgniteThreadFactory(gridName)
        );
    }

    /**
     * Creates a new service with the given initial parameters.
     *
     * @param gridName Name of the grid.
     * @param corePoolSize The number of threads to keep in the pool, even if they are idle.
     * @param maxPoolSize The maximum number of threads to allow in the pool.
     * @param keepAliveTime When the number of threads is greater than the core, this is the maximum time
     *      that excess idle threads will wait for new tasks before terminating.
     * @param workQ The queue to use for holding tasks before they are executed. This queue will hold only the
     *      runnable tasks submitted by the {@link #execute(Runnable)} method.
     * @param hnd Optional handler to use when execution is blocked because the thread bounds and queue
     *      capacities are reached. If {@code null} then {@code AbortPolicy}
     *      handler is used by default.
     */
    public IgniteThreadPoolExecutor(
        String gridName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        RejectedExecutionHandler hnd) {
        this(
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            workQ,
            new IgniteThreadFactory(gridName),
            hnd
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
     * @param hnd Optional handler to use when execution is blocked because the thread bounds and queue
     *      capacities are reached. If {@code null} then {@code AbortPolicy}
     *      handler is used by default.
     */
    public IgniteThreadPoolExecutor(
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        ThreadFactory threadFactory,
        @Nullable RejectedExecutionHandler hnd) {
        super(
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            TimeUnit.MILLISECONDS,
            workQ,
            threadFactory,
            hnd == null ? new AbortPolicy() : hnd
        );
    }
}