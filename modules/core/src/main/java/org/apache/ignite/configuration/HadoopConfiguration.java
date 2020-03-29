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

import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.hadoop.HadoopMapReducePlanner;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite Hadoop Accelerator configuration.
 */
public class HadoopConfiguration {
    /** Default finished job info time-to-live. */
    public static final long DFLT_FINISHED_JOB_INFO_TTL = 30_000;

    /** Default value for external execution flag. */
    public static final boolean DFLT_EXTERNAL_EXECUTION = false;

    /** Default value for the max parallel tasks. */
    public static final int DFLT_MAX_PARALLEL_TASKS = Runtime.getRuntime().availableProcessors() * 2;

    /** Default value for the max task queue size. */
    public static final int DFLT_MAX_TASK_QUEUE_SIZE = 8192;

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

    /** Library names. */
    private String[] libNames;

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
        libNames = cfg.getNativeLibraryNames();
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
     * @return {@code this} for chaining.
     */
    public HadoopConfiguration setMaxParallelTasks(int maxParallelTasks) {
        this.maxParallelTasks = maxParallelTasks;

        return this;
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
     * @return {@code this} for chaining.
     */
    public HadoopConfiguration setMaxTaskQueueSize(int maxTaskQueueSize) {
        this.maxTaskQueueSize = maxTaskQueueSize;

        return this;
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
     * @return {@code this} for chaining.
     */
    public HadoopConfiguration setFinishedJobInfoTtl(long finishedJobInfoTtl) {
        this.finishedJobInfoTtl = finishedJobInfoTtl;

        return this;
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
     * @return {@code this} for chaining.
     */
    // TODO: IGNITE-404: Uncomment when fixed.
//
//    public HadoopConfiguration setExternalExecution(boolean extExecution) {
//        this.extExecution = extExecution;
//
//        return this;
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
     * @return {@code this} for chaining.
     */
    public HadoopConfiguration setMapReducePlanner(HadoopMapReducePlanner planner) {
        this.planner = planner;

        return this;
    }

    /**
     * Get native library names.
     * <p>
     * Ignite Hadoop Accelerator executes all Hadoop jobs and tasks in the same process, isolating them with help
     * of classloaders. If Hadoop job or task loads a native library, it might lead to exception, because Java do
     * not allow to load the same library multiple times from different classloaders. To overcome the problem,
     * you should to the following:
     * <ul>
     *     <li>Load necessary libraries in advance from base classloader; {@link LifecycleBean} is a good candidate
     *     for this;</li>
     *     <li>Add names of loaded libraries to this property, so that Hadoop engine is able to link them;</li>
     *     <li>Remove {@link System#load(String)} and {@link System#loadLibrary(String)} calls from your job/task.</li>     *
     * </ul>
     *
     * @return Native library names.
     */
    @Nullable public String[] getNativeLibraryNames() {
        return libNames;
    }

    /**
     * Set native library names. See {@link #getNativeLibraryNames()} for more information.
     *
     * @param libNames Native library names.
     * @return {@code this} for chaining.
     */
    public HadoopConfiguration setNativeLibraryNames(@Nullable String... libNames) {
        this.libNames = libNames;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopConfiguration.class, this);
    }
}
