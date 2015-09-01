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

package org.apache.ignite.configuration;

import org.apache.ignite.internal.processors.hadoop.HadoopMapReducePlanner;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Ignite Hadoop Accelerator configuration.
 */
public class HadoopConfiguration {
    /** Default finished job info time-to-live. */
    public static final long DFLT_FINISHED_JOB_INFO_TTL = 10_000;

    /** Default value for external execution flag. */
    public static final boolean DFLT_EXTERNAL_EXECUTION = false;

    /** Default value for the max parallel tasks. */
    public static final int DFLT_MAX_PARALLEL_TASKS = Runtime.getRuntime().availableProcessors();

    /** Default value for the max task queue size. */
    public static final int DFLT_MAX_TASK_QUEUE_SIZE = 1000;

    /** Map reduce planner. */
    private HadoopMapReducePlanner planner;

    /** */
    private boolean extExecution = DFLT_EXTERNAL_EXECUTION;

    /** Finished job info TTL. */
    private long finishedJobInfoTtl = DFLT_FINISHED_JOB_INFO_TTL;

    /** */
    private int maxParallelTasks = DFLT_MAX_PARALLEL_TASKS;

    /** */
    private int maxTaskQueueSize = DFLT_MAX_TASK_QUEUE_SIZE;

    /**
     * Default constructor.
     */
    public HadoopConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to copy.
     */
    public HadoopConfiguration(HadoopConfiguration cfg) {
        // Preserve alphabetic order.
        // TODO: IGNITE-404: Uncomment when fixed.
        //extExecution = cfg.isExternalExecution();
        finishedJobInfoTtl = cfg.getFinishedJobInfoTtl();
        planner = cfg.getMapReducePlanner();
        maxParallelTasks = cfg.getMaxParallelTasks();
        maxTaskQueueSize = cfg.getMaxTaskQueueSize();
    }

    /**
     * Gets max number of local tasks that may be executed in parallel.
     *
     * @return Max number of local tasks that may be executed in parallel.
     */
    public int getMaxParallelTasks() {
        return maxParallelTasks;
    }

    /**
     * Sets max number of local tasks that may be executed in parallel.
     *
     * @param maxParallelTasks Max number of local tasks that may be executed in parallel.
     */
    public void setMaxParallelTasks(int maxParallelTasks) {
        this.maxParallelTasks = maxParallelTasks;
    }

    /**
     * Gets max task queue size.
     *
     * @return Max task queue size.
     */
    public int getMaxTaskQueueSize() {
        return maxTaskQueueSize;
    }

    /**
     * Sets max task queue size.
     *
     * @param maxTaskQueueSize Max task queue size.
     */
    public void setMaxTaskQueueSize(int maxTaskQueueSize) {
        this.maxTaskQueueSize = maxTaskQueueSize;
    }

    /**
     * Gets finished job info time-to-live in milliseconds.
     *
     * @return Finished job info time-to-live.
     */
    public long getFinishedJobInfoTtl() {
        return finishedJobInfoTtl;
    }

    /**
     * Sets finished job info time-to-live.
     *
     * @param finishedJobInfoTtl Finished job info time-to-live.
     */
    public void setFinishedJobInfoTtl(long finishedJobInfoTtl) {
        this.finishedJobInfoTtl = finishedJobInfoTtl;
    }

    /**
     * Gets external task execution flag. If {@code true}, hadoop job tasks will be executed in an external
     * (relative to node) process.
     *
     * @return {@code True} if external execution.
     */
    // TODO: IGNITE-404: Uncomment when fixed.
//    public boolean isExternalExecution() {
//        return extExecution;
//    }

    /**
     * Sets external task execution flag.
     *
     * @param extExecution {@code True} if tasks should be executed in an external process.
     * @see #isExternalExecution()
     */
    // TODO: IGNITE-404: Uncomment when fixed.
//    public void setExternalExecution(boolean extExecution) {
//        this.extExecution = extExecution;
//    }

    /**
     * Gets Hadoop map-reduce planner, a component which defines job execution plan based on job
     * configuration and current grid topology.
     *
     * @return Map-reduce planner.
     */
    public HadoopMapReducePlanner getMapReducePlanner() {
        return planner;
    }

    /**
     * Sets Hadoop map-reduce planner, a component which defines job execution plan based on job
     * configuration and current grid topology.
     *
     * @param planner Map-reduce planner.
     */
    public void setMapReducePlanner(HadoopMapReducePlanner planner) {
        this.planner = planner;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopConfiguration.class, this, super.toString());
    }
}