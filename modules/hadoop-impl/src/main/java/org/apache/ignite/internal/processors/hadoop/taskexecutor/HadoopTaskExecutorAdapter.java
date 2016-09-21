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

package org.apache.ignite.internal.processors.hadoop.taskexecutor;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopComponent;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.jobtracker.HadoopJobMetadata;

/**
 * Common superclass for task executor.
 */
public abstract class HadoopTaskExecutorAdapter extends HadoopComponent {
    /**
     * Runs tasks.
     *
     * @param job Job.
     * @param tasks Tasks.
     * @throws IgniteCheckedException If failed.
     */
    public abstract void run(final HadoopJob job, Collection<HadoopTaskInfo> tasks) throws IgniteCheckedException;

    /**
     * Cancels all currently running tasks for given job ID and cancels scheduled execution of tasks
     * for this job ID.
     * <p>
     * It is guaranteed that this method will not be called concurrently with
     * {@link #run(org.apache.ignite.internal.processors.hadoop.HadoopJob, Collection)} method. No more job submissions will be performed via
     * {@link #run(org.apache.ignite.internal.processors.hadoop.HadoopJob, Collection)} method for given job ID after this method is called.
     *
     * @param jobId Job ID to cancel.
     */
    public abstract void cancelTasks(HadoopJobId jobId) throws IgniteCheckedException;

    /**
     * On job state change callback;
     *
     * @param meta Job metadata.
     */
    public abstract void onJobStateChanged(HadoopJobMetadata meta) throws IgniteCheckedException;
}