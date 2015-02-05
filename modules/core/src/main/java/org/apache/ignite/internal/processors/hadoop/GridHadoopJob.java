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

import org.apache.ignite.*;

import java.util.*;

/**
 * Hadoop job.
 */
public interface GridHadoopJob {
    /**
     * Gets job ID.
     *
     * @return Job ID.
     */
    public GridHadoopJobId id();

    /**
     * Gets job information.
     *
     * @return Job information.
     */
    public GridHadoopJobInfo info();

    /**
     * Gets collection of input splits for this job.
     *
     * @return Input splits.
     */
    public Collection<GridHadoopInputSplit> input() throws IgniteCheckedException;

    /**
     * Returns context for task execution.
     *
     * @param info Task info.
     * @return Task Context.
     * @throws IgniteCheckedException If failed.
     */
    public GridHadoopTaskContext getTaskContext(GridHadoopTaskInfo info) throws IgniteCheckedException;

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
    public void initialize(boolean external, UUID locNodeId) throws IgniteCheckedException;

    /**
     * Release all the resources.
     * <p>
     * If job is running in external mode this method will be called on instance in Ignite node with parameter
     * {@code false} and on instance in external process with parameter {@code true}.
     *
     * @param external If {@code true} then this job instance resides in external process.
     * @throws IgniteCheckedException If failed.
     */
    public void dispose(boolean external) throws IgniteCheckedException;

    /**
     * Prepare local environment for the task.
     *
     * @param info Task info.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareTaskEnvironment(GridHadoopTaskInfo info) throws IgniteCheckedException;

    /**
     * Cleans up local environment of the task.
     *
     * @param info Task info.
     * @throws IgniteCheckedException If failed.
     */
    public void cleanupTaskEnvironment(GridHadoopTaskInfo info) throws IgniteCheckedException;

    /**
     * Cleans up the job staging directory.
     */
    void cleanupStagingDirectory();
}
