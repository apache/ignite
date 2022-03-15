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

package org.apache.ignite.internal.processors.pool;

import org.apache.ignite.internal.processors.metric.MetricRegistry;

/**
 * Executor service that supports self-registration of metrics.
 */
public interface MetricsAwareExecutorService {
    /** Tas execution time metric name. */
    public String TASK_EXEC_TIME_NAME = "TaskExecTime";

    /** Tas execution time metric description. */
    public String TASK_EXEC_TIME_DESC = "Task execution time, in milliseconds.";

    /** */
    public String ACTIVE_COUNT_DESC = "Approximate number of threads that are actively executing tasks.";

    /** */
    public String COMPLETED_TASK_DESC = "Approximate total number of tasks that have completed execution.";

    /** */
    public String CORE_SIZE_DESC = "The core number of threads.";

    /** */
    public String LARGEST_SIZE_DESC = "Largest number of threads that have ever simultaneously been in the pool.";

    /** */
    public String MAX_SIZE_DESC = "The maximum allowed number of threads.";

    /** */
    public String POOL_SIZE_DESC = "Current number of threads in the pool.";

    /** */
    public String TASK_COUNT_DESC = "Approximate total number of tasks that have been scheduled for execution.";

    /** */
    public String QUEUE_SIZE_DESC = "Current size of the execution queue.";

    /** */
    public String KEEP_ALIVE_TIME_DESC = "Thread keep-alive time, which is the amount of time which threads in excess of " +
        "the core pool size may remain idle before being terminated.";

    /** */
    public String IS_SHUTDOWN_DESC = "True if this executor has been shut down.";

    /** */
    public String IS_TERMINATED_DESC = "True if all tasks have completed following shut down.";

    /** */
    public String IS_TERMINATING_DESC = "True if terminating but not yet terminated.";

    /** */
    public String REJ_HND_DESC = "Class name of current rejection handler.";

    /** */
    public String THRD_FACTORY_DESC = "Class name of thread factory used to create new threads.";

    /** */
    public long[] TASK_EXEC_TIME_HISTOGRAM_BUCKETS = new long[] {
        500,
        1000,
        5000,
        30000,
        60000
    };

    /**
     * Register thread pool metrics.
     *
     * @param mreg Metrics registry.
     */
    public void registerMetrics(MetricRegistry mreg);
}
