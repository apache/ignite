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
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.util.future.GridFinishedFuture;

/**
 * Hadoop processor.
 */
public class HadoopNoopProcessor extends HadoopProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    public HadoopNoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Hadoop hadoop() {
        throw new IllegalStateException("Hadoop module is not found in class path.");
    }

    /** {@inheritDoc} */
    @Override public HadoopConfiguration config() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public HadoopJobId nextJobId() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> submit(HadoopJobId jobId, HadoopJobInfo jobInfo) {
        return new GridFinishedFuture<>(new IgniteCheckedException("Hadoop is not available."));
    }

    /** {@inheritDoc} */
    @Override public HadoopJobStatus status(HadoopJobId jobId) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public HadoopCounters counters(HadoopJobId jobId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> finishFuture(HadoopJobId jobId) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean kill(HadoopJobId jobId) throws IgniteCheckedException {
        return false;
    }
}