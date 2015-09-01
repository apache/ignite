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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;

/**
 * Hadoop processor.
 */
public abstract class HadoopProcessorAdapter extends GridProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    protected HadoopProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @return Hadoop facade.
     */
    public abstract Hadoop hadoop();

    /**
     * @return Hadoop configuration.
     */
    public abstract HadoopConfiguration config();

    /**
     * @return Collection of generated IDs.
     */
    public abstract HadoopJobId nextJobId();

    /**
     * Submits job to job tracker.
     *
     * @param jobId Job ID to submit.
     * @param jobInfo Job info to submit.
     * @return Execution future.
     */
    public abstract IgniteInternalFuture<?> submit(HadoopJobId jobId, HadoopJobInfo jobInfo);

    /**
     * Gets Hadoop job execution status.
     *
     * @param jobId Job ID to get status for.
     * @return Job execution status.
     * @throws IgniteCheckedException If failed.
     */
    public abstract HadoopJobStatus status(HadoopJobId jobId) throws IgniteCheckedException;

    /**
     * Returns Hadoop job counters.
     *
     * @param jobId Job ID to get counters for.
     * @return Job counters.
     * @throws IgniteCheckedException If failed.
     */
    public abstract HadoopCounters counters(HadoopJobId jobId) throws IgniteCheckedException;

    /**
     * Gets Hadoop job finish future.
     *
     * @param jobId Job ID.
     * @return Job finish future or {@code null}.
     * @throws IgniteCheckedException If failed.
     */
    public abstract IgniteInternalFuture<?> finishFuture(HadoopJobId jobId) throws IgniteCheckedException;

    /**
     * Kills job.
     *
     * @param jobId Job ID.
     * @return {@code True} if job was killed.
     * @throws IgniteCheckedException If failed.
     */
    public abstract boolean kill(HadoopJobId jobId) throws IgniteCheckedException;
}