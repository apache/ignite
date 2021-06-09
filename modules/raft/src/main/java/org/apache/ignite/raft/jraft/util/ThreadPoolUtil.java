/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class ThreadPoolUtil {
    /**
     * The default rejected execution handler
     */
    private static final RejectedExecutionHandler defaultHandler = new ThreadPoolExecutor.AbortPolicy();

    public static PoolBuilder newBuilder() {
        return new PoolBuilder();
    }

    public static ScheduledPoolBuilder newScheduledBuilder() {
        return new ScheduledPoolBuilder();
    }

    /**
     * Creates a new {@code MetricThreadPoolExecutor} or {@code LogThreadPoolExecutor} with the given initial parameters
     * and default rejected execution handler.
     *
     * @param poolName the name of the thread pool
     * @param enableMetric if metric is enabled
     * @param coreThreads the number of threads to keep in the pool, even if they are idle, unless {@code
     * allowCoreThreadTimeOut} is set.
     * @param maximumThreads the maximum number of threads to allow in the pool
     * @param keepAliveSeconds when the number of threads is greater than the core, this is the maximum time (seconds)
     * that excess idle threads will wait for new tasks before terminating.
     * @param workQueue the queue to use for holding tasks before they are executed. This queue will hold only the
     * {@code Runnable} tasks submitted by the {@code execute} method.
     * @param threadFactory the factory to use when the executor creates a new thread
     * @throws IllegalArgumentException if one of the following holds:<br> {@code corePoolSize < 0}<br> {@code
     * keepAliveSeconds < 0}<br> {@code maximumPoolSize <= 0}<br> {@code maximumPoolSize < corePoolSize}
     * @throws NullPointerException if {@code workQueue} or {@code threadFactory} or {@code handler} is null
     */
    public static ThreadPoolExecutor newThreadPool(final String poolName, final boolean enableMetric,
        final int coreThreads, final int maximumThreads,
        final long keepAliveSeconds,
        final BlockingQueue<Runnable> workQueue,
        final ThreadFactory threadFactory) {
        return newThreadPool(poolName, enableMetric, coreThreads, maximumThreads, keepAliveSeconds, workQueue,
            threadFactory, defaultHandler);
    }

    /**
     * Creates a new {@code MetricThreadPoolExecutor} or {@code LogThreadPoolExecutor} with the given initial
     * parameters.
     *
     * @param poolName the name of the thread pool
     * @param enableMetric if metric is enabled
     * @param coreThreads the number of threads to keep in the pool, even if they are idle, unless {@code
     * allowCoreThreadTimeOut} is set.
     * @param maximumThreads the maximum number of threads to allow in the pool
     * @param keepAliveSeconds when the number of threads is greater than the core, this is the maximum time (seconds)
     * that excess idle threads will wait for new tasks before terminating.
     * @param workQueue the queue to use for holding tasks before they are executed. This queue will hold only the
     * {@code Runnable} tasks submitted by the {@code execute} method.
     * @param threadFactory the factory to use when the executor creates a new thread
     * @param rejectedHandler the handler to use when execution is blocked because the thread bounds and queue
     * capacities are reached
     * @throws IllegalArgumentException if one of the following holds:<br> {@code corePoolSize < 0}<br> {@code
     * keepAliveSeconds < 0}<br> {@code maximumPoolSize <= 0}<br> {@code maximumPoolSize < corePoolSize}
     * @throws NullPointerException if {@code workQueue} or {@code threadFactory} or {@code handler} is null
     */
    public static ThreadPoolExecutor newThreadPool(final String poolName, final boolean enableMetric,
        final int coreThreads, final int maximumThreads,
        final long keepAliveSeconds,
        final BlockingQueue<Runnable> workQueue,
        final ThreadFactory threadFactory,
        final RejectedExecutionHandler rejectedHandler) {
        final TimeUnit unit = TimeUnit.SECONDS;
        if (enableMetric) {
            return new MetricThreadPoolExecutor(coreThreads, maximumThreads, keepAliveSeconds, unit, workQueue,
                threadFactory, rejectedHandler, poolName);
        }
        else {
            return new LogThreadPoolExecutor(coreThreads, maximumThreads, keepAliveSeconds, unit, workQueue,
                threadFactory, rejectedHandler, poolName);
        }
    }

    /**
     * Creates a new ScheduledThreadPoolExecutor with the given initial parameters.
     *
     * @param poolName the name of the thread pool
     * @param enableMetric if metric is enabled
     * @param coreThreads the number of threads to keep in the pool, even if they are idle, unless {@code
     * allowCoreThreadTimeOut} is set.
     * @param threadFactory the factory to use when the executor creates a new thread
     * @return a new ScheduledThreadPoolExecutor
     * @throws IllegalArgumentException if {@code corePoolSize < 0}
     * @throws NullPointerException if {@code threadFactory} or {@code handler} is null
     */
    public static ScheduledThreadPoolExecutor newScheduledThreadPool(final String poolName, final boolean enableMetric,
        final int coreThreads,
        final ThreadFactory threadFactory) {
        return newScheduledThreadPool(poolName, enableMetric, coreThreads, threadFactory, defaultHandler);
    }

    /**
     * Creates a new ScheduledThreadPoolExecutor with the given initial parameters.
     *
     * @param poolName the name of the thread pool
     * @param enableMetric if metric is enabled
     * @param coreThreads the number of threads to keep in the pool, even if they are idle, unless {@code
     * allowCoreThreadTimeOut} is set.
     * @param threadFactory the factory to use when the executor creates a new thread
     * @param rejectedHandler the handler to use when execution is blocked because the thread bounds and queue
     * capacities are reached
     * @return a new ScheduledThreadPoolExecutor
     * @throws IllegalArgumentException if {@code corePoolSize < 0}
     * @throws NullPointerException if {@code threadFactory} or {@code handler} is null
     */
    public static ScheduledThreadPoolExecutor newScheduledThreadPool(final String poolName, final boolean enableMetric,
        final int coreThreads,
        final ThreadFactory threadFactory,
        final RejectedExecutionHandler rejectedHandler) {
        if (enableMetric) {
            return new MetricScheduledThreadPoolExecutor(coreThreads, threadFactory, rejectedHandler, poolName);
        }
        else {
            return new LogScheduledThreadPoolExecutor(coreThreads, threadFactory, rejectedHandler, poolName);
        }
    }

    private ThreadPoolUtil() {
    }

    public static class PoolBuilder {
        private String poolName;
        private Boolean enableMetric;
        private Integer coreThreads;
        private Integer maximumThreads;
        private Long keepAliveSeconds;
        private BlockingQueue<Runnable> workQueue;
        private ThreadFactory threadFactory;
        private RejectedExecutionHandler handler = ThreadPoolUtil.defaultHandler;

        public PoolBuilder poolName(final String poolName) {
            this.poolName = poolName;
            return this;
        }

        public PoolBuilder enableMetric(final Boolean enableMetric) {
            this.enableMetric = enableMetric;
            return this;
        }

        public PoolBuilder coreThreads(final Integer coreThreads) {
            this.coreThreads = coreThreads;
            return this;
        }

        public PoolBuilder maximumThreads(final Integer maximumThreads) {
            this.maximumThreads = maximumThreads;
            return this;
        }

        public PoolBuilder keepAliveSeconds(final Long keepAliveSeconds) {
            this.keepAliveSeconds = keepAliveSeconds;
            return this;
        }

        public PoolBuilder workQueue(final BlockingQueue<Runnable> workQueue) {
            this.workQueue = workQueue;
            return this;
        }

        public PoolBuilder threadFactory(final ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public PoolBuilder rejectedHandler(final RejectedExecutionHandler handler) {
            this.handler = handler;
            return this;
        }

        public ThreadPoolExecutor build() {
            Requires.requireNonNull(this.poolName, "poolName");
            Requires.requireNonNull(this.enableMetric, "enableMetric");
            Requires.requireNonNull(this.coreThreads, "coreThreads");
            Requires.requireNonNull(this.maximumThreads, "maximumThreads");
            Requires.requireNonNull(this.keepAliveSeconds, "keepAliveSeconds");
            Requires.requireNonNull(this.workQueue, "workQueue");
            Requires.requireNonNull(this.threadFactory, "threadFactory");
            Requires.requireNonNull(this.handler, "handler");

            return ThreadPoolUtil.newThreadPool(this.poolName, this.enableMetric, this.coreThreads,
                this.maximumThreads, this.keepAliveSeconds, this.workQueue, this.threadFactory, this.handler);
        }
    }

    public static class ScheduledPoolBuilder {
        private String poolName;
        private Boolean enableMetric;
        private Integer coreThreads;
        private ThreadFactory threadFactory;
        private RejectedExecutionHandler handler = ThreadPoolUtil.defaultHandler;

        public ScheduledPoolBuilder poolName(final String poolName) {
            this.poolName = poolName;
            return this;
        }

        public ScheduledPoolBuilder enableMetric(final Boolean enableMetric) {
            this.enableMetric = enableMetric;
            return this;
        }

        public ScheduledPoolBuilder coreThreads(final Integer coreThreads) {
            this.coreThreads = coreThreads;
            return this;
        }

        public ScheduledPoolBuilder threadFactory(final ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public ScheduledPoolBuilder rejectedHandler(final RejectedExecutionHandler handler) {
            this.handler = handler;
            return this;
        }

        public ScheduledThreadPoolExecutor build() {
            Requires.requireNonNull(this.poolName, "poolName");
            Requires.requireNonNull(this.enableMetric, "enableMetric");
            Requires.requireNonNull(this.coreThreads, "coreThreads");

            Requires.requireNonNull(this.threadFactory, "threadFactory");
            Requires.requireNonNull(this.handler, "handler");

            return ThreadPoolUtil.newScheduledThreadPool(this.poolName, this.enableMetric, this.coreThreads,
                this.threadFactory, this.handler);
        }
    }
}
