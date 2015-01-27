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

package org.apache.ignite.mxbean;

/**
 * MBean that provides access to information about executor service.
 */
@IgniteMXBeanDescription("MBean that provides access to information about executor service.")
public interface IgniteThreadPoolMXBean {
    /**
     * Returns the approximate number of threads that are actively executing tasks.
     *
     * @return The number of threads.
     */
    @IgniteMXBeanDescription("Approximate number of threads that are actively executing tasks.")
    public int getActiveCount();

    /**
     * Returns the approximate total number of tasks that have completed execution.
     * Because the states of tasks and threads may change dynamically during
     * computation, the returned value is only an approximation, but one that
     * does not ever decrease across successive calls.
     *
     * @return The number of tasks.
     */
    @IgniteMXBeanDescription("Approximate total number of tasks that have completed execution.")
    public long getCompletedTaskCount();

    /**
     * Returns the core number of threads.
     *
     * @return The core number of threads.
     */
    @IgniteMXBeanDescription("The core number of threads.")
    public int getCorePoolSize();

    /**
     * Returns the largest number of threads that have ever
     * simultaneously been in the pool.
     *
     * @return The number of threads.
     */
    @IgniteMXBeanDescription("Largest number of threads that have ever simultaneously been in the pool.")
    public int getLargestPoolSize();

    /**
     * Returns the maximum allowed number of threads.
     *
     * @return The maximum allowed number of threads.
     */
    @IgniteMXBeanDescription("The maximum allowed number of threads.")
    public int getMaximumPoolSize();

    /**
     * Returns the current number of threads in the pool.
     *
     * @return The number of threads.
     */
    @IgniteMXBeanDescription("Current number of threads in the pool.")
    public int getPoolSize();

    /**
     * Returns the approximate total number of tasks that have been scheduled
     * for execution. Because the states of tasks and threads may change dynamically
     * during computation, the returned value is only an approximation, but
     * one that does not ever decrease across successive calls.
     *
     * @return The number of tasks.
     */
    @IgniteMXBeanDescription("Approximate total number of tasks that have been scheduled for execution.")
    public long getTaskCount();

    /**
     * Gets current size of the execution queue. This queue buffers local
     * executions when there are not threads available for processing in the pool.
     *
     * @return Current size of the execution queue.
     */
    @IgniteMXBeanDescription("Current size of the execution queue.")
    public int getQueueSize();

    /**
     * Returns the thread keep-alive time, which is the amount of time which threads
     * in excess of the core pool size may remain idle before being terminated.
     *
     * @return Keep alive time.
     */
    @IgniteMXBeanDescription("Thread keep-alive time, which is the amount of time which threads in excess of " +
        "the core pool size may remain idle before being terminated.")
    public long getKeepAliveTime();

    /**
     * Returns {@code true} if this executor has been shut down.
     *
     * @return {@code True} if this executor has been shut down.
     */
    @IgniteMXBeanDescription("True if this executor has been shut down.")
    public boolean isShutdown();

    /**
     * Returns {@code true} if all tasks have completed following shut down. Note that
     * {@code isTerminated()} is never {@code true} unless either {@code shutdown()} or
     * {@code shutdownNow()} was called first.
     *
     * @return {@code True} if all tasks have completed following shut down.
     */
    @IgniteMXBeanDescription("True if all tasks have completed following shut down.")
    public boolean isTerminated();

    /**
     * Returns {@code true} if this executor is in the process of terminating after
     * {@code shutdown()} or {@code shutdownNow()} but has not completely terminated.
     * This method may be useful for debugging. A return of {@code true} reported a
     * sufficient period after shutdown may indicate that submitted tasks have ignored
     * or suppressed interruption, causing this executor not to properly terminate.
     *
     * @return {@code True} if terminating but not yet terminated.
     */
    @IgniteMXBeanDescription("True if terminating but not yet terminated.")
    public boolean isTerminating();

    /**
     * Returns the class name of current rejection handler.
     *
     * @return Class name of current rejection handler.
     */
    @IgniteMXBeanDescription("Class name of current rejection handler.")
    public String getRejectedExecutionHandlerClass();

    /**
     * Returns the class name of thread factory used to create new threads.
     *
     * @return Class name of thread factory used to create new threads.
     */
    @IgniteMXBeanDescription("Class name of thread factory used to create new threads.")
    public String getThreadFactoryClass();
}
