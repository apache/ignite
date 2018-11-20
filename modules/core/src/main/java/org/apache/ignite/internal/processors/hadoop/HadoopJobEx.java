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

package org.apache.ignite.internal.processors.hadoop;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.hadoop.HadoopInputSplit;
import org.apache.ignite.hadoop.HadoopJob;
import org.jetbrains.annotations.Nullable;

/**
 * Hadoop job.
 */
public abstract class HadoopJobEx implements HadoopJob {
    /**
     * Gets job ID.
     *
     * @return Job ID.
     */
    abstract public HadoopJobId id();

    /**
     * Gets job information.
     *
     * @return Job information.
     */
    abstract public HadoopJobInfo info();

    /**
     * Gets collection of input splits for this job.
     *
     * @return Input splits.
     */
    @Override abstract public Collection<HadoopInputSplit> input();

    /**
     * Returns context for task execution.
     *
     * @param info Task info.
     * @return Task Context.
     * @throws IgniteCheckedException If failed.
     */
    abstract public HadoopTaskContext getTaskContext(HadoopTaskInfo info) throws IgniteCheckedException;

    /**
     * Does all the needed initialization for the job. Will be called on each node where tasks for this job must
     * be executed.
     * <p>
     * If job is running in external mode this method will be called on instance in Ignite node with parameter
     * {@code false} and on instance in external process with parameter {@code true}.
     *
     * @param external If {@code true} then this job instance resides in external process.
     * @param locNodeId Local node ID.
     * @throws IgniteCheckedException If failed.
     */
    abstract public void initialize(boolean external, UUID locNodeId) throws IgniteCheckedException;

    /**
     * Release all the resources.
     * <p>
     * If job is running in external mode this method will be called on instance in Ignite node with parameter
     * {@code false} and on instance in external process with parameter {@code true}.
     *
     * @param external If {@code true} then this job instance resides in external process.
     * @throws IgniteCheckedException If failed.
     */
    abstract public void dispose(boolean external) throws IgniteCheckedException;

    /**
     * Prepare local environment for the task.
     *
     * @param info Task info.
     * @throws IgniteCheckedException If failed.
     */
    abstract public void prepareTaskEnvironment(HadoopTaskInfo info) throws IgniteCheckedException;

    /**
     * Cleans up local environment of the task.
     *
     * @param info Task info.
     * @throws IgniteCheckedException If failed.
     */
    abstract public void cleanupTaskEnvironment(HadoopTaskInfo info) throws IgniteCheckedException;

    /**
     * Cleans up the job staging directory.
     */
    abstract public void cleanupStagingDirectory();

    /**
     * @return Ignite work directory.
     */
    abstract public String igniteWorkDirectory();

    /** {@inheritDoc} */
    @Nullable @Override public String property(String name) {
        return info().property(name);
    }

    /** {@inheritDoc} */
    @Override public boolean hasCombiner() {
        return info().hasCombiner();
    }

    /** {@inheritDoc} */
    @Override public boolean hasReducer() {
        return info().hasReducer();
    }

    /** {@inheritDoc} */
    @Override public int reducers() {
        return info().reducers();
    }

    /** {@inheritDoc} */
    @Override public String jobName() {
        return info().jobName();
    }

    /** {@inheritDoc} */
    @Override public String user() {
        return info().user();
    }
}